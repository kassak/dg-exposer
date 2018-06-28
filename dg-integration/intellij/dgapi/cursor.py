from intellij.dgapi.exceptions import Error, DatabaseError
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
        if self._desc is None:
            self._desc = _parse_desc(_handle_error(self._dg.describe(self._con._ds, self._con._con, self._cursor)))
        return self._desc

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
        return _deserialize_rows(_handle_error(self._dg.fetch(self._con._ds, self._con._con, self._cursor, limit)))

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


def _format_parameters(params):
    return [
        {'value': p, 'type': 'S'}
        for p in params
    ]


def _handle_error(data):
    if 'error' in data:
        raise DatabaseError(data['error'])
    return data


def _deserialize_rows(rows):
    return [_deserialize_row(r) for r in rows]


def _deserialize_row(row):
    return row


def _parse_desc(desc):
    return [(
        d.get('name'),
        _parse_type(d.get('type')),
        None,
        None,
        d.get('precision'),
        d.get('scale'),
        None
    ) for d in desc]


def _parse_type(type):
    # todo
    return STRING
