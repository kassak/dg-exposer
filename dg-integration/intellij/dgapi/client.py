class DGClient(object):
    def __init__(self, client):
        self._c = client

    def data_sources(self):
        return self._c.perform_json("database/dataSources/")

    def data_source(self, ds):
        return self._c.perform_json("database/dataSources/{0}/".format(ds['uuid']))

    def connections(self, ds):
        return self._c.perform_json("database/dataSources/{0}/connections/".format(ds['uuid']))

    def connect(self, ds, **kwargs):
        r = self._c.request("database/dataSources/{0}/connections/".format(ds['uuid']))
        r.method = "POST"
        if kwargs:
            pass #todo: autocommit
        return self._c.perform_json(r)

    def close_connection(self, ds, con):
        r = self._c.request("database/dataSources/{0}/connections/{1}/".format(ds['uuid'], con['uuid']))
        r.method = "DELETE"
        return self._c.perform_json(r)

    def commit(self, ds, con):
        r = self._c.request("database/dataSources/{0}/connections/{1}/commit".format(ds['uuid'], con['uuid']))
        r.method = "POST"
        return self._c.perform_json(r)

    def rollback(self, ds, con):
        r = self._c.request("database/dataSources/{0}/connections/{1}/rollback".format(ds['uuid'], con['uuid']))
        r.method = "POST"
        return self._c.perform_json(r)

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
