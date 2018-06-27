from intellij.dgapi.client import DGClient
from intellij.discover import any_instance, Client as BClient

apilevel = "2.0"
threadsafety = 0
paramstyle = "qmark"


class Error(Exception):
    pass


class Warning(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


def connect(**kwargs):
    return Connection(**kwargs)


_fallback_client = None


def _get_fallback_client():
    global _fallback_client
    if _fallback_client is None: _fallback_client = DGClient(any_instance())
    return _fallback_client


class Connection(object):
    def __init__(self, **kwargs):
        self._dg = kwargs['inst'] if 'inst' in kwargs else None
        if self._dg is None:
            self._dg = _fallback_client
        elif isinstance(self._dg, BClient):
            self._dg = DGClient(self._dg)
        elif not isinstance(self._dg, DGClient):
            raise InterfaceError("Not expecting {0} as 'inst'".format(self._dg))

        if 'ds' in kwargs:
            self._ds = kwargs['ds']
            if 'uuid' not in self._ds:
                raise InterfaceError("Data source should have uuid: got {0}".format(self._ds))
        elif 'dsn' in kwargs:
            name = kwargs['dsn']
            self._ds = next((ds for ds in self._dg.data_sources() if ds['name'] == name), None)
        elif 'dsid' in kwargs:
            uuid = kwargs['dsid']
            self._ds = next((ds for ds in self._dg.data_sources() if ds['uuid'] == uuid), None)
        else:
            raise InterfaceError("No data source coordinates provided")
        if self._ds is None:
            raise InterfaceError("No data source found")

        self._con = self._dg.connect(self._ds, autocommit=False)

    def close(self):
        if self._con is None:
            raise Error("Connection closed")
        self._close()

    def commit(self):
        self._dg.commit(self._ds, self._con)

    def rollback(self):
        self._dg.rollback(self._ds, self._con)

    def cursor(self):
        pass #todo:

    def __del__(self):
        self._close()

    def _close(self):
        if self._con is not None:
            self._dg.close_connection(self._ds, self._con)
            self._con = None
