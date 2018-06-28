import json


class DGClient(object):
    def __init__(self, client):
        self._c = client

    def data_sources(self):
        return self._perform("database/dataSources/")

    def data_source(self, ds):
        return self._perform(self._mk_request("database/dataSources/{0}/", ds))

    def connections(self, ds):
        return self._perform(self._mk_request("database/dataSources/{0}/connections/", ds))

    def connect(self, ds, **kwargs):
        r = self._mk_request("database/dataSources/{0}/connections/", ds)
        r.method = "POST"
        if kwargs:
            pass  # todo: autocommit
        return self._perform(r)

    def close_connection(self, ds, con):
        r = self._mk_request("database/dataSources/{0}/connections/{1}/", ds, con)
        r.method = "DELETE"
        return self._perform(r)

    def commit(self, ds, con):
        r = self._mk_request("database/dataSources/{0}/connections/{1}/commit", ds, con)
        r.method = "POST"
        return self._perform(r)

    def rollback(self, ds, con):
        r = self._mk_request("database/dataSources/{0}/connections/{1}/rollback", ds, con)
        r.method = "POST"
        return self._perform(r)

    def create_cursor(self, ds, con):
        r = self._mk_request("database/dataSources/{0}/connections/{1}/cursors/", ds, con)
        r.method = "POST"
        return self._perform(r)

    def close_cursor(self, ds, con, cur):
        r = self._mk_request("database/dataSources/{0}/connections/{1}/cursors/{2}/", ds, con, cur)
        r.method = "DELETE"
        return self._perform(r)

    def execute(self, ds, con, cur, operation, parameters):
        r = self._mk_request("database/dataSources/{0}/connections/{1}/cursors/{2}/execute", ds, con, cur)
        r.method = "POST"
        body = {'parameters': parameters}
        if operation is not None:
            body['operation'] = operation
        r.data = bytes(json.dumps(body), 'utf8')
        return self._perform(r)

    def fetch(self, ds, con, cur, limit):
        r = self._mk_request("database/dataSources/{0}/connections/{1}/cursors/{2}/fetch", ds, con, cur,
                             limit=limit)
        return self._perform(r)

    def nextset(self, ds, con, cur):
        r = self._mk_request("database/dataSources/{0}/connections/{1}/cursors/{2}/nextSet", ds, con, cur)
        r.method = "POST"
        return self._perform(r)

    def _perform(self, r):
        return self._c.perform_json(r)

    def _mk_request(self, s, *objs, **kwargs):
        url = s.format(*[o['uuid'] for o in objs])
        params = "&".join("{0}={1}".format(k, v) for k, v in kwargs.items() if v is not None)
        if len(params) != 0:
            url += "?" + params
        return self._c.request(url)

    def __repr__(self):
        return "<DB:{0}>".format(repr(self._c))


if __name__ == '__main__':
    from intellij.discover import any_instance, discover_running_instances

    print(discover_running_instances())

    base = any_instance()
    base.noisy = True
    c = DGClient(base)
    print(c)
    _ds = c.data_sources()
    print(_ds)
    _ds = _ds[0]
    print(_ds)
    print(c.connections(_ds))
    _con = c.connect(_ds)
    print(_con)
    print(c.connections(_ds))
    print(c.rollback(_ds, _con))
    print(c.commit(_ds, _con))
    print(c.close_connection(_ds, _con))
