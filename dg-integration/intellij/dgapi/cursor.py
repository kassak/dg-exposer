from datetime import date, time, datetime
from numbers import Number

import dateutil.parser

from intellij.dgapi.exceptions import Error, DatabaseError, OperationalError
from intellij.dgapi.types import *


# noinspection PyProtectedMember
class Cursor(object):
    def __init__(self, con, cur):
        self._dg = con._dg
        self._con = con
        self._cursor = cur
        self._last_rc = -1
        self._desc = None
        self.arraysize = 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close()

    @property
    def connection(self):
        return self._con

    @property
    def description(self):
        self._ensure_desc()
        return self._desc

    def _ensure_desc(self):
        if self._desc is None:
            self._desc = _parse_desc(_handle_error(self._dg.describe(self._con._ds, self._con._con, self._cursor)))

    @property
    def rowcount(self):
        return self._last_rc

    # def callproc(self):
    #     pass

    def close(self):
        if self._con is None:
            raise Error("Cursor closed")
        self._close()

    def __del__(self):
        self._close()

    def _close(self):
        if self._cursor is None:
            return
        self._dg.close_cursor(self._con._ds, self._con._con, self._cursor)
        self._cursor = None
        self._con = None

    def execute(self, operation, parameters=(), **kwargs):
        if operation is None:
            raise Error('Operation should not be None')
        if 'commit' == operation:
            self._con.commit()
            self._last_rc = -1
            return
        res = self._execute(operation, parameters)
        self._last_rc = res['rowcount']

    def executemany(self, operation, seq_of_parameters, **kwargs):
        if operation is None:
            raise Error('Operation should not be None')
        for i, parameters in enumerate(seq_of_parameters):
            self._execute(operation if i == 0 else None, parameters)

    def _execute(self, operation, parameters):
        self._desc = None
        return _handle_error(self._dg.execute(self._con._ds, self._con._con, self._cursor, operation,
                                              _format_parameters(parameters)))

    def _fetch(self, limit):
        self._ensure_desc()
        return _deserialize_rows(_handle_error(self._dg.fetch(self._con._ds, self._con._con, self._cursor, limit)), self.description)

    def fetchone(self):
        res = self._fetch(1)
        return res[0] if res else None

    def fetchmany(self, size=None):
        return self._fetch(size if size is not None else self.arraysize)

    def fetchall(self):
        return self._fetch(None)

    def nextset(self):
        self._desc = None
        res = _handle_error(self._dg.nextset(self._con._ds, self._con._con, self._cursor))
        more = res['more'] if 'more' in res else False
        return True if more else None

    def setinputsizes(self, sizes):
        pass

    def setoutputsizes(self, sizes, column=None):
        pass

    def next(self):
        val = self.fetchone()
        if val is None:
            raise StopIteration()
        return val

    def __iter__(self):
        return self


def _handle_error(data):
    if 'error' in data:
        kind = data.get('kind')
        if kind == 'O':
            raise OperationalError(data['error'])
        raise DatabaseError(data['error'])
    return data


def _deserialize_rows(rows, desc):
    return [_deserialize_row(r, desc) for r in rows]


def _deserialize_row(row, desc):
    return [_deserialize_val(v, d[1]) for v, d in zip(row, desc)]


def _deserialize_val(val, t):
    if val is None:
        return val
    if t == NUMBER:
        try:
            return float(val)  # todo
        except:
            return val

    if t == BINARY:
        return bytes(val, 'latin1')
    if t == DATETIME:
        try:
            return dateutil.parser.parse(val)
        except:
            return val
    return val


def _parse_desc(desc):
    return [(
        d.get('name'),
        _parse_type(d.get('type')),
        None,
        None,
        d.get('precision'),
        d.get('scale'),
        None
    ) for d in desc] if desc else None


def _format_parameters(params):
    return [
        _format_parameter(p)
        for p in params
    ]


def _format_parameter(p):
    tp = _guess_type(p)
    return {'value': str(p), 'type': _get_type(tp)}


def _parse_type(type):
    global _types
    return _types.get(type, STRING)


def _get_type(code):
    global _types
    next((k for k, v in _types if v == code), 'S')


def _guess_type(val):
    if isinstance(val, Number):
        return NUMBER
    if isinstance(val, date) or isinstance(val, datetime) or isinstance(val, time):
        return DATETIME
    if isinstance(val, bytes):
        return BINARY
    return STRING


_types = {
    'S': STRING,
    'N': NUMBER,
    'D': DATETIME,
    'R': ROWID,
    'B': BINARY,
}
