from intellij.discover import Client as BClient, any_instance
from intellij.dgapi.client import DGClient
from intellij.dgapi.exceptions import InterfaceError, Error
from intellij.dgapi.cursor import Cursor, _handle_error

_fallback_client = None


def _get_fallback_client():
    global _fallback_client
    if _fallback_client is None:
        _fallback_client = DGClient(any_instance())
    return _fallback_client


class Connection(object):
    def __init__(self, **kwargs):
        self._dg = None
        self._ds = None
        self._con = None
        self._setup(kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close()

    def _setup(self, kwargs):
        self._dg = kwargs['inst'] if 'inst' in kwargs else _get_fallback_client()
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
            if 'create' in kwargs:
                url = kwargs['create']
                while self._ds is not None and self._ds['url'] != url:
                    self._dg.delete_data_source(self._ds)
                    self._ds = next((ds for ds in self._dg.data_sources() if ds['name'] == name), None)
                if self._ds is None:
                    self._ds = self._dg.create_data_source(name=name, url=url)
        elif 'dsid' in kwargs:
            uuid = kwargs['dsid']
            self._ds = next((ds for ds in self._dg.data_sources() if ds['uuid'] == uuid), None)
        else:
            raise InterfaceError("No data source coordinates provided")
        if self._ds is None:
            raise InterfaceError("No data source found")
        self._con = self._handle_error(self._dg.connect(self._ds, autocommit=False))

    def close(self):
        if self._con is None:
            raise Error("Connection closed")
        self._close()

    def dbms(self):
        return self._ds['dbms']

    def commit(self):
        self._handle_error(self._dg.commit(self._ds, self._con))

    def rollback(self):
        self._handle_error(self._dg.rollback(self._ds, self._con))

    def cursor(self):
        cur = self._handle_error(self._dg.create_cursor(self._ds, self._con))
        return Cursor(self, cur)

    def __del__(self):
        self._close()

    def _close(self):
        if self._con is not None:
            r = self._dg.close_connection(self._ds, self._con)
            self._con = None
            self._handle_error(r)

    def _handle_error(self, data):
        return _handle_error(data, self._dg._c.noisy)
