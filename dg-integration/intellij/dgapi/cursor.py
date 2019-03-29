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
        return None if self._desc is None else [
            (c[0], c[1][1], None, None, c[2], c[3], None)
            for c in self._desc
        ]

    def _ensure_desc(self):
        if self._desc is None:
            self._desc = _parse_desc(self._handle_error(self._dg.describe(self._con._ds, self._con._con, self._cursor)))

    @property
    def rowcount(self):
        return self._last_rc

    @property
    def lastrowid(self):
        return None

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
        return self._handle_error(self._dg.execute(self._con._ds, self._con._con, self._cursor, operation,
                                                   _format_parameters(parameters)))

    def _fetch(self, limit):
        self._ensure_desc()
        return _deserialize_rows(self._handle_error(self._dg.fetch(self._con._ds, self._con._con, self._cursor, limit)),
                                 self._desc)

    def fetchone(self):
        res = self._fetch(1)
        return res[0] if res else None

    def fetchmany(self, size=None):
        return self._fetch(size if size is not None else self.arraysize)

    def fetchall(self):
        return self._fetch(None)

    def nextset(self):
        self._desc = None
        res = self._handle_error(self._dg.nextset(self._con._ds, self._con._con, self._cursor))
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

    def _handle_error(self, data):
        return _handle_error(data, self._dg._c.noisy)


def _handle_error(data, noisy):
    if 'error' in data:
        e = data['error']
        kind = data.get('kind')
        if noisy and 'trace' in data:
            print(data['trace'])
        if kind == 'O':
            raise OperationalError(e)
        raise DatabaseError(e)
    return data


def _deserialize_rows(rows, desc):
    return [_deserialize_row(r, desc) for r in rows]


def _deserialize_row(row, desc):
    return [_deserialize_val(v, d) for v, d in zip(row, desc)]


def _deserialize_val(val, d):
    t = d[1]
    if val is None:
        return val
    try:
        if t == _INT:
            return int(val)
        if t == _BOOL:
            return int(val) == 1
        if t == _NUM:
            return float(val)
        if t == _BIN:
            return bytes(val, 'latin1')
        if t == _DATE:
            return dateutil.parser.parse(val).date()
        if t == _DATETIME:
            return dateutil.parser.parse(val)
        if t == _TIME:
            return dateutil.parser.parse(val).time()
        return val
    except:
        return val


def _parse_desc(desc):
    return [(
        d.get('name'),
        _parse_type(d.get('type')),
        d.get('precision'),
        d.get('scale'),
    ) for d in desc] if desc else None


def _format_parameters(params):
    return [
        _format_parameter(p)
        for p in params
    ]


def _format_parameter(p):
    tp = _guess_type(p)
    return {'value': _format_val(p), 'type': tp[0]}


def _format_val(p):
    if p is None:
        return None
    if isinstance(p, bool):
        return '1' if p else '0'
    return str(p)


def _parse_type(code):
    global _types
    return next((t for t in _types if t[0] == code), _STR)


def _guess_type(val):
    import datetime as dt
    from numbers import Number

    if isinstance(val, bool):
        return _BOOL
    if isinstance(val, int):
        return _INT
    if isinstance(val, Number):
        return _NUM
    if isinstance(val, dt.datetime):
        return _DATETIME
    if isinstance(val, dt.date):
        return _DATE
    if isinstance(val, dt.time):
        return _TIME
    if isinstance(val, bytes):
        return _BIN
    return _STR


_STR = ('S', STRING)
_BOOL = ('1', NUMBER)
_NUM = ('N', NUMBER)
_INT = ('I', NUMBER)
_DATE = ('D', DATETIME)
_DATETIME = ('d', DATETIME)
_TIME = ('T', DATETIME)
_ROWID = ('R', ROWID)
_BIN = ('B', BINARY)

_types = [_STR, _NUM, _INT, _DATE, _DATETIME, _TIME, _ROWID, _BIN, _BOOL]
