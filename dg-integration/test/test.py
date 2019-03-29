import unittest
from intellij.dgapi import *
from intellij.dgapi.client import DGClient
from intellij.discover import any_instance, create_instance, discover_running_instances, Client


def find_test_app():
    insts = discover_running_instances()
    print(insts)
    for inst in insts:
        c = Client(inst.host, inst.port)
        try:
            c.perform('TestApp')
            return c
        except Exception as e:
            print('Not test:', inst, e)
    return run_test_app()


def run_test_app():
    trigger = 'TestApp is running on port: '
    import subprocess, os, atexit
    proc = subprocess.Popen(['gradle', 'test'], cwd=os.path.relpath('../dg-exposer', os.path.abspath(__file__)),
                            stdout=subprocess.PIPE)
    atexit.register(proc.terminate)
    for line in proc.stdout:
        line = line.decode("utf-8")
        idx = line.find(trigger)
        if idx != -1:
            port = int(line[idx + len(trigger):])
            print(trigger, port)
            return create_instance('127.0.0.1', port)


def create_or_replace_ds(inst, **kwargs):
    client = DGClient(inst)
    name = kwargs['name']
    ex = next((ds for ds in client.data_sources() if ds['name'] == name), None)
    if ex is not None:
        client.delete_data_source(ex)
    return client.create_data_source(**kwargs)


class TestDBAPI(unittest.TestCase):
    _test_instance = find_test_app()
    _test_instance.noisy = True

    _sqlite = create_or_replace_ds(_test_instance, name='identifier.sqlite', url='jdbc:sqlite:identifier.sqlite')
    _h2 = create_or_replace_ds(_test_instance, name='h2', url='jdbc:h2:mem:db')
    _pg = create_or_replace_ds(_test_instance, name='pg', url='jdbc:postgresql://localhost:54330/guest',
                               user='guest', password='guest')

    @staticmethod
    def connect(ds):
        return connect(dsn=ds['name'], inst=TestDBAPI._test_instance)

    def test_simple(self):
        with self.connect(self._sqlite) as c:
            with c.cursor() as cur:
                cur.execute('drop table if exists a')
                cur.execute('create table a(a int)')
                c.commit()
                c.rollback()

    def test_fetch_one(self):
        with self.connect(self._sqlite) as c:
            with c.cursor() as cur:
                cur.execute('select ?, ?', ('mama', 'papa'))
                self.assertEqual(['mama', 'papa'], cur.fetchone())
                self.assertIsNone(cur.fetchone())

    def test_fetch_one_1(self):
        with self.connect(self._sqlite) as c:
            with c.cursor() as cur:
                cur.execute('select \'mama\', \'papa\'')
                self.assertEqual(['mama', 'papa'], cur.fetchone())
                self.assertIsNone(cur.fetchone())

    def test_fetch_one_2(self):
        with self.connect(self._sqlite) as c:
            with c.cursor() as cur:
                cur.execute('select ? union select ?', ('mama', 'papa'))
                self.assertEqual(['mama'], cur.fetchone())
                self.assertEqual(['papa'], cur.fetchone())
                self.assertIsNone(cur.fetchone())

    def test_fetch_all(self):
        with self.connect(self._sqlite) as c:
            with c.cursor() as cur:
                cur.execute('select ? union select ?', ('mama', 'papa'))
                self.assertEqual([['mama'], ['papa']], cur.fetchall())
                self.assertEqual([], cur.fetchall())

    def test_nextset(self):
        with self.connect(self._sqlite) as c:
            with c.cursor() as cur:
                cur.execute('select ?, ?', ('mama', 'papa'))
                self.assertIsNone(cur.nextset())

    def test_describe(self):
        with self.connect(self._sqlite) as c:
            with c.cursor() as cur:
                cur.execute('select ? as m, ? as p', ('mama', 'papa'))
                self.assertIsNotNone(cur.description)
                self.assertEqual(['m', 'p'], [p[0] for p in cur.description])

    def test_commit2(self):
        with self.connect(self._sqlite) as c:
            c.commit()
            c.commit()

    def test_type(self):
        with self.connect(self._h2) as c:
            with c.cursor() as cur:
                cur.execute('select now()')
                n = cur.fetchone()[0]
                self.assertIsInstance(n, datetime.datetime)

    def test_date_time(self):
        with self.connect(self._pg) as c:
            with c.cursor() as cur:
                bd = datetime.datetime(1991, 4, 7, 0, 40)
                cur.execute('select ?::timestamp', (bd,))
                n = cur.fetchone()[0]
                self.assertIsInstance(n, datetime.datetime)
                self.assertEqual(bd, n)

    def test_time(self):
        with self.connect(self._pg) as c:
            with c.cursor() as cur:
                bd = datetime.time(0, 40)
                cur.execute('select ?::time', (bd,))
                n = cur.fetchone()[0]
                self.assertIsInstance(n, datetime.time)
                self.assertEqual(bd, n)


if __name__ == '__main__':
    unittest.main()
