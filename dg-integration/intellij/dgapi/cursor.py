from intellij.dgapi import Error, DatabaseError


# noinspection PyProtectedMember
class Cursor(object):
    def __init__(self, con, cur):
        self._dg = con._dg
        self._con = con
        self._cursor = cur
        self._last_rc = -1
        self.arraysize = 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close()

    @property
    def description(self):
        if self._cursor is None:
            return None
        # todo

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
            res = self._execute(operation if i == 0 else None, parameters)

    def _execute(self, operation, parameters):
        return _handle_error(self._dg.execute(self._con._ds, self._con._con, self._cursor, operation, _format_parameters(parameters)))


    def _check_rs(self):
        if self._rs is None:
            raise Error("No result set")

    def fetchone(self):
        self._check_rs()
        # todo

    def fetchmany(self, size=None):
        self._check_rs()
        if size is None:
            size = self.arraysize
        # todo

    def fetchall(self):
        self._check_rs()
        # todo

    def nextset(self):
        if self._stmt is None:
            raise Error("No statement")
        self._cleanup_rs()
        # todo

    def setinputsizes(self, sizes):
        pass

    def setoutputsizes(self, sizes, column=None):
        pass


def _format_parameters(params):
    return [
        {'value': p, 'type': 'S'}
        for p in params
    ]

def _handle_error(data):
    if 'error' in data:
        raise DatabaseError(data['error'])
    return data