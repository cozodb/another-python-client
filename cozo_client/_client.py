import base64
import hashlib
import re
from enum import Enum
import logging
import html
import json
from typing import Any, Generator, Union, Callable

import requests
import cozo_embedded
from pydantic import BaseModel

from ._builder import InputProgram, ConstantRule, RuleHead, InlineRule, Param, RuleApply, StoredRuleNamedApply, \
    StoreOp, InputRelation

logger = logging.getLogger(__name__)

ROW_DISPLAY_MAX_LEN = 100


def _hash_ids(ids):
    hash_object = hashlib.sha256(b''.join(value.encode() for value in ids))
    urlsafe_hash = base64.urlsafe_b64encode(hash_object.digest()).rstrip(
        b'=').decode()
    return urlsafe_hash


class StorageEngine(str, Enum):
    mem = 'mem'
    rocksdb = 'rocksdb'
    sqlite = 'sqlite'
    remote = 'remote'


class CozoResponse(BaseModel):
    headers: list[str]
    rows: list[list[Any]]
    max_display_rows: int = 50

    def _repr_html_(self):
        html_str = ["<table>", "<thead>", "<tr>", "<th></th>"]

        for header in self.headers:
            html_str.append(f"<th style='font-size: 80%; text-align: center;'>{html.escape(header)}</th>")

        html_str.append("</tr>")
        html_str.append("</thead>")
        html_str.append("<tbody>")

        for i, row in enumerate(self.rows[:self.max_display_rows]):
            html_str.append(f"<tr><td style='font-family: monospace; font-size: 80%; opacity: 50%'>{i}</td>")

            for col in row:
                if isinstance(col, str):
                    t = col
                    if len(t) > ROW_DISPLAY_MAX_LEN:
                        t = t[:ROW_DISPLAY_MAX_LEN - 3] + '...'
                    html_str.append(f"<td style='text-align: left;'>{html.escape(t)}</td>")
                else:
                    t = json.dumps(col, ensure_ascii=False)
                    if len(t) > ROW_DISPLAY_MAX_LEN:
                        t = t[:ROW_DISPLAY_MAX_LEN - 3] + '...'
                    html_str.append(f"<td style='font-family: monospace;'>{html.escape(t)}</td>")

            html_str.append("</tr>")

        html_str.append("</tbody>")
        html_str.append("</table>")
        if len(self.rows) > self.max_display_rows:
            html_str.append(
                f"<p style='opacity: 50%; font-size: 80%;'>"
                f"Displaying first {self.max_display_rows} rows out of {len(self.rows)}.</p>")

        return ''.join(html_str)

    def __getitem__(self, item) -> Union['CozoRow', 'CozoResponse']:
        match item:
            case int():
                return CozoRow(headers=self.headers, row=self.rows[item])
            case slice():
                return CozoResponse(headers=self.headers, rows=self.rows[item])
            case list():
                coll = []
                for i in item:
                    assert isinstance(i, int)
                    coll.append(self.rows[i])
                return CozoResponse(headers=self.headers, rows=coll)
            case _:
                raise ValueError(f"Invalid index type: {type(item)}")

    def get(self, item, default=None) -> Union['CozoRow', 'CozoResponse', None]:
        try:
            return self[item]
        except IndexError:
            return default

    def __len__(self):
        return len(self.rows)

    def __iter__(self) -> Generator['CozoRow', None, None]:
        for i in range(len(self.rows)):
            yield self[i]

    def to_list(self) -> list[dict[str, Any]]:
        return [{self.headers[i]: row[i] for i in range(len(self.headers))} for row in self.rows]

    def cozo_filter(self, filters: str | list[str], orders: list[str] | None = None, limit: int | None = None,
                    offset: int | None = None) -> 'CozoResponse':
        db = CozoClient()
        if not isinstance(filters, str):
            filters = ', '.join(filters)
        header_str = ', '.join(self.headers)
        q = f'''
            rows[] <- $rows
            ?[{header_str}] := rows[{header_str}], {filters}
        '''
        if orders:
            q += '\n :order ' + ', '.join(orders)
        if limit:
            q += '\n :limit ' + str(limit)
        if offset:
            q += '\n :offset ' + str(offset)
        return db.run(q, {'rows': self.rows})

    def filter(self, filter_fn: Callable) -> 'CozoResponse':
        new_rows = []
        for i, row in enumerate(self):
            if filter_fn(row.to_dict()):
                new_rows.append(self.rows[i])
        return CozoResponse(headers=self.headers, rows=new_rows)

    def flatmap(self, flatmap_fn) -> Union['CozoResponse', None]:
        new_rows = []
        headers = None
        for row in self:
            for item in flatmap_fn(row):
                if headers is None:
                    headers = list(item.keys())
                new_rows.append([item[header] for header in headers])
        if headers is None:
            return None
        return CozoResponse(headers=headers, rows=new_rows)

    def map(self, map_fn) -> Union['CozoResponse', None]:
        new_rows = []
        headers = None
        for row in self:
            item = map_fn(row)
            if headers is None:
                headers = list(item.keys())
            new_rows.append([item[header] for header in headers])
        return CozoResponse(headers=headers, rows=new_rows)

    def concat(self, other: 'CozoResponse', *rest: 'CozoResponse', merge_columns=False):
        if merge_columns:
            new_headers = self.headers.copy()
            for h in other.headers:
                if h not in new_headers:
                    new_headers.append(h)
        else:
            new_headers = []
            for h in self.headers:
                if h in other.headers:
                    new_headers.append(h)
        inv_headers = {h: i for i, h in enumerate(new_headers)}
        new_rows = []
        for row in self.rows:
            new_row = [None] * len(new_headers)
            for i, val in enumerate(row):
                new_row[inv_headers[self.headers[i]]] = val
            new_rows.append(new_row)
        for row in other.rows:
            new_row = [None] * len(new_headers)
            for i, val in enumerate(row):
                new_row[inv_headers[other.headers[i]]] = val
            new_rows.append(new_row)
        res = CozoResponse(headers=new_headers, rows=new_rows)
        if rest:
            return res.concat(*rest, merge_columns=merge_columns)
        else:
            return res

    def join(self, *args, **kwargs):
        headers = self.headers.copy()
        inv_headers = {h: i for i, h in enumerate(headers)}
        data = {row['id']: row.to_dict() for row in self}

        for arg in args:
            if not isinstance(arg, CozoResponse):
                raise ValueError(f"Invalid argument type: {type(arg)}")
            for h in arg.headers:
                if h not in headers:
                    inv_headers[h] = len(headers)
                    headers.append(h)
            for row in arg:
                data.setdefault(row['id']).update(row.to_dict())
        for k, arg in kwargs.items():
            if not isinstance(arg, CozoResponse):
                raise ValueError(f"Invalid argument type: {type(arg)}")
            for h in arg.headers:
                if h != 'id':
                    inv_headers['k.' + h] = len(headers)
                    headers.append('k.' + h)
            for row in arg:
                data.setdefault(row['id']).update({'k.' + k: v for k, v in row.to_dict().items()})
        new_rows = []
        for row in data.values():
            new_rows.append([row.get(k) for k in headers])
        return CozoResponse(headers=headers, rows=new_rows)

    def mini_batch(self, max_n: int) -> 'CozoResponse':
        if 'ids' in self.headers:
            raise ValueError("Cannot mini_batch a CozoResponse that already has an 'ids' column")
        new_rows = []
        cur_row: dict[str, Any] = {}
        headers = self.headers + ['ids']
        for row in self:
            if len(cur_row) >= max_n:
                cur_row['id'] = _hash_ids(cur_row['ids'])
                new_rows.append([cur_row.get(k) for k in headers])
                cur_row = {}

            for k, v in row.to_dict().items():
                if k == 'id':
                    k = 'ids'
                cur_row.setdefault(k, []).append(v)
        if cur_row:
            cur_row['id'] = _hash_ids(cur_row['ids'])
            new_rows.append([cur_row.get(k) for k in headers])
        return CozoResponse(headers=headers, rows=new_rows)

    def un_batch(self):
        def generic_unbatch(row: CozoRow):
            n = len(row['ids'])
            for i in range(n):
                new_row = {'id': row['ids'][i]}
                for k, v in row.to_dict().items():
                    if k not in ('ids', 'id'):
                        new_row[k] = v[i]
                yield new_row

        return self.flatmap(generic_unbatch)

    def save_as_spreadsheet(self, filename: str):
        from openpyxl import Workbook
        from openpyxl.styles import Font

        wb = Workbook()
        ws = wb.active
        ws.append(self.headers)
        for row in self.rows:
            nr = []
            for v in row:
                if isinstance(v, str) or isinstance(v, int) or isinstance(v, float):
                    nr.append(v)
                else:
                    nr.append(json.dumps(v, ensure_ascii=False))
            ws.append(nr)

        bold_font = Font(bold=True)
        for cell in ws["1:1"]:
            cell.font = bold_font

        wb.save(filename)


class CozoRow(BaseModel):
    headers: list[str]
    row: list[Any]

    def _repr_html_(self):
        html_str = ["<table>", "<tbody>"]
        for i, header in enumerate(self.headers):
            val = self.row[i]
            html_str.append("<tr>")
            html_str.append(
                f"<td style='font-weight: bold; font-size: 80%;'>{html.escape(header)}:</td>")
            if isinstance(val, str):
                html_str.append(f"<td style='text-align: left;'>{html.escape(val)}</td>")
            else:
                t = html.escape(json.dumps(val, ensure_ascii=False, indent=2))
                html_str.append(f"<td style='text-align: left; font-family: monospace;'>{t}</td>")
            html_str.append("</tr>")
        html_str.append("</tbody>")
        html_str.append("</table>")
        return ''.join(html_str)

    def __getitem__(self, item: int | str):
        if isinstance(item, str):
            for i, header in enumerate(self.headers):
                if header == item:
                    item = i
                    break
        if isinstance(item, str):
            raise KeyError(item)
        return self.row[item]

    def get(self, item, default=None):
        try:
            return self[item]
        except KeyError:
            return default

    def __setitem__(self, key, value):
        if isinstance(key, str):
            for i, header in enumerate(self.headers):
                if header == key:
                    key = i
                    break
        if isinstance(key, str):
            raise KeyError(key)
        self.row[key] = value

    def __len__(self):
        return len(self.headers)

    def __str__(self):
        return str(self.to_dict())

    def to_dict(self):
        return {k: v for k, v in zip(self.headers, self.row)}


class CozoClient:
    def __init__(self,
                 path: str | None = None,
                 engine: StorageEngine = StorageEngine.mem,
                 remote_host: str = 'http://127.0.0.1:9070/',
                 remote_token: str | None = None,
                 remote_auth: str | None = None):

        if engine == StorageEngine.mem and path is not None:
            engine = StorageEngine.sqlite

        self.engine = engine

        if engine == StorageEngine.remote:
            if remote_host.endswith('/'):
                remote_host = remote_host[:len(remote_host) - 1]
            self.host = remote_host
            self.embedded = None
            self._remote_sse = {}
            self._remote_cb_id = 0
            self._session = requests.Session()
            if remote_token:
                self._session.headers.update({'Authorization': f'Bearer {remote_token}'})
            elif remote_auth:
                self._session.headers.update({'x-cozo-auth': remote_auth})
        else:
            self.embedded = cozo_embedded.CozoDbPy(engine, path or '', '{}')

    def ensure_index(self, name: str, idx_name: str, cols: list[str]):
        try:
            existing_cols = self.run(f'::columns {name}:{idx_name}')
            for i, row in enumerate(existing_cols):
                assert row['column'] == cols[i], f'Column {i} is {row["column"]} not {cols[i]}'

        except QueryException as e:
            if e.code == 'query::relation_not_found':
                prog = f'''::index create {name}:{idx_name} {{ {', '.join(cols)} }}'''
                self.run(prog)
            else:
                raise

    def ensure_relation(self,
                        name: str,
                        keys: list[str | tuple[str, str] | tuple[str, str, str]],
                        vals: list[str | tuple[str, str] | tuple[str, str, str]]):
        def get_ident(d):
            match d:
                case str(d):
                    return d
                case tuple(d):
                    return d[0]
                case _:
                    raise ValueError(f'Invalid column definition: {d}')

        def get_type(d):
            match d:
                case str(_):
                    return 'Any?'
                case tuple(d):
                    return d[1]
                case _:
                    raise ValueError(f'Invalid column definition: {d}')

        try:
            existing_cols = self.run(f'::columns {name}')
            assert len([row for row in existing_cols if row['is_key']]) == len(keys)
            assert len([row for row in existing_cols if not row['is_key']]) == len(vals)
            for i, col in enumerate(keys):
                assert get_ident(col) == existing_cols[i]['column']
                assert get_type(col) == existing_cols[i]['type']
            col_map = {row['column']: row for row in existing_cols if not row['is_key']}
            for col in vals:
                assert get_ident(col) in col_map
                assert get_type(col) == col_map[get_ident(col)]['type']
        except QueryException as e:
            if e.code == 'query::relation_not_found':
                prog = InputProgram(
                    store_relation=(StoreOp.CREATE, InputRelation(name=name, keys=keys, values=vals)))
                self.run(prog)
            else:
                raise

    def ensure_lsh(self,
                   rel_name: str,
                   idx_name: str,
                   extractor: str,
                   filter: str | None,
                   n_perm: int = 200,
                   n_gram: int = 7,
                   target_threshold: float = 0.5):
        try:
            self.run(f'''::columns {rel_name}:{idx_name}''')
        except QueryException as e:
            if e.code == 'query::relation_not_found':
                query = f'''::lsh create {rel_name}:{idx_name} {{
                    extractor: {extractor},
                    {'filter: ' + filter + ',' if filter else ''}
                    tokenizer: NGram,
                    n_perm: {n_perm},
                    target_threshold: {target_threshold},
                    n_gram: {n_gram},
                }}'''
                self.run(query)
            else:
                raise

    def ensure_vec_index(self, rel_name: str, idx_name: str, dim: int, m: int, ef: int):
        try:
            self.run(f'::columns {rel_name}:{idx_name}')
        except QueryException as e:
            if e.code == 'query::relation_not_found':
                self.run(f'''::hnsw create {rel_name}:{idx_name} {{
                    dim: $dim,
                    m: $m,
                    dtype: F32,
                    fields: vec,
                    distance: Cosine,
                    ef: $ef,
                }}''', {'dim': dim, 'm': m, 'ef': ef})
            else:
                raise

    def __eq__(self, other):
        if isinstance(other, CozoClient):
            if self.is_remote and other.is_remote:
                return self.host == other.host
            elif not self.is_remote and not other.is_remote:
                return self.embedded == other.embedded
        return False

    def __del__(self):
        try:
            self.close()
        except:
            pass

    @property
    def is_remote(self):
        return self.embedded is None

    def close(self):
        if self.embedded:
            self.embedded.close()

    def _client_request(self, script, params=None, immutable=False):

        r = self._session.post(f'{self.host}/text-query', json={
            'script': script,
            'params': params or {},
            'immutable': immutable
        })
        res = r.json()
        return self._format_return(res)

    @staticmethod
    def _format_return(res):
        if not res['ok']:
            raise QueryException(res)

        return CozoResponse(**res)

    def _embedded_request(self, script, params=None, immutable=False):
        try:
            res = self.embedded.run_script(script, params or {}, immutable)
        except Exception as e:
            raise QueryException(e.args[0])
        return CozoResponse(**res)

    def run(self,
            script: str | InputProgram,
            params: dict[str, Any] | None = None,
            replace: dict[str, str] | None = None,
            immutable=False
            ) -> CozoResponse:
        script = str(script)
        if replace:
            for k, v in replace.items():
                r = r'\$\$' + k + r'\$\$'
                script = re.sub(r, v, script)
        if self.is_remote:
            return self._client_request(script, params, immutable)
        else:
            return self._embedded_request(script, params, immutable)

    def export_relations(self, relations) -> dict[str, CozoResponse]:
        if not self.is_remote:
            return {k: CozoResponse(**v) for k, v in self.embedded.export_relations(relations).items()}
        else:
            import urllib.parse

            rels = ','.join(map(lambda s: urllib.parse.quote_plus(s), relations))
            url = f'{self.host}/export/{rels}'

            r = requests.get(url)
            res = r.json()
            if res['ok']:
                return {k: CozoResponse(**v) for k, v in res['data'].items()}
            else:
                raise RuntimeError(res['message'])

    def import_relations(self, data):
        if self.embedded:
            self.embedded.import_relations(data)
        else:
            import requests
            url = f'{self.host}/import'

            r = requests.put(url, json=data)
            res = r.json()
            if not res['ok']:
                raise RuntimeError(res['message'])

    def backup(self, path):
        if self.embedded:
            self.embedded.backup(path)
        else:
            import requests

            r = requests.post(f'{self.host}/backup', json={'path': path})
            res = r.json()
            if not res['ok']:
                raise RuntimeError(res['message'])

    def restore(self, path):
        if self.embedded:
            self.embedded.restore(path)
        else:
            raise RuntimeError('Remote databases cannot be restored remotely')

    @staticmethod
    def _process_mutate_data_dict(data):
        cols = []
        row = []
        for k, v in data.items():
            cols.append(k)
            row.append(v)
        return cols, row

    def _process_mutate_data(self, data):
        assert not isinstance(data, str)
        if isinstance(data, dict):
            cols, row = self._process_mutate_data_dict(data)
            return cols, [row]
        elif isinstance(data, list):
            cols, row = self._process_mutate_data_dict(data[0])
            rows = [row]
            for el in data[1:]:
                nxt_row = []
                for col in cols:
                    nxt_row.append(el[col])
                rows.append(nxt_row)
            return cols, rows
        else:
            raise RuntimeError('Invalid data type for mutation')

    def _mutate(self, relation, data, op, returning):
        cols, processed_data = self._process_mutate_data(data)
        cols_str = ', '.join(cols)
        q = f'?[{cols_str}] <- $data :{op} {relation} {{ {cols_str} }}'
        if returning:
            q += ' :returning'
        return self.run(q, {'data': processed_data})

    def insert(self, relation: str, data: dict[str, Any] | list[dict[str, Any]], returning: bool = False):
        return self._mutate(relation, data, 'insert', returning)

    def put(self, relation: str, data: dict[str, Any] | list[dict[str, Any]], returning: bool = False):
        return self._mutate(relation, data, 'put', returning)

    def update(self, relation: str, data: dict[str, Any] | list[dict[str, Any]], returning: bool = False):
        return self._mutate(relation, data, 'update', returning)

    def rm(self, relation: str, data: dict[str, Any] | list[dict[str, Any]], returning: bool = False):
        return self._mutate(relation, data, 'rm', returning)

    def delete(self, relation: str, data: dict[str, Any] | list[dict[str, Any]], returning: bool = False):
        return self._mutate(relation, data, 'delete', returning)

    def get(self, relation: str, data: dict[str, Any] | list[dict[str, Any]], out: list[str] | None = None):
        if out is None:
            out = [row['column'] for row in self.run(f'::columns {relation}')]

        cols, processed_data = self._process_mutate_data(data)
        prog = InputProgram(
            rules=[
                ConstantRule(head=RuleHead(name='data', args=cols), body=Param(name='data')),
                InlineRule(head=RuleHead(name='?', args=out),
                           atoms=[RuleApply(name='data', args=cols),
                                  StoredRuleNamedApply(name=relation, args={k: k for k in out})]),
            ],
        )
        return self.run(prog, {'data': processed_data})


class QueryException(Exception):
    def __init__(self, resp):
        super().__init__()
        self.resp = resp

    def __str__(self):
        return self.resp.get('display') or self.resp.get('message') or str(self.resp)

    def _repr_pretty_(self, p, _cycle):
        p.text(repr(self))

    @property
    def code(self):
        return self.resp.get('code')
