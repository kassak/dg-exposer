import unittest
from intellij.dgapi import *
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


class TestDBAPI(unittest.TestCase):
    _test_instance = find_test_app()
    _test_instance.noisy = True

    @staticmethod
    def connect(dsn):
        return connect(dsn=dsn, inst=TestDBAPI._test_instance)

    def test_simple(self):
        with self.connect('identifier.sqlite') as c:
            with c.cursor() as cur:
                cur.execute('drop table if exists a')
                cur.execute('create table a(a int)')
                c.commit()
                c.rollback()

    def test_fetch_one(self):
        with self.connect('identifier.sqlite') as c:
            with c.cursor() as cur:
                cur.execute('select ?, ?', ('mama', 'papa'))
                self.assertEqual(['mama', 'papa'], cur.fetchone())
                self.assertIsNone(cur.fetchone())

    def test_fetch_one_1(self):
        with self.connect('identifier.sqlite') as c:
            with c.cursor() as cur:
                cur.execute('select \'mama\', \'papa\'')
                self.assertEqual(['mama', 'papa'], cur.fetchone())
                self.assertIsNone(cur.fetchone())

    def test_fetch_one_2(self):
        with self.connect('identifier.sqlite') as c:
            with c.cursor() as cur:
                cur.execute('select ? union select ?', ('mama', 'papa'))
                self.assertEqual(['mama'], cur.fetchone())
                self.assertEqual(['papa'], cur.fetchone())
                self.assertIsNone(cur.fetchone())

    def test_fetch_all(self):
        with self.connect('identifier.sqlite') as c:
            with c.cursor() as cur:
                cur.execute('select ? union select ?', ('mama', 'papa'))
                self.assertEqual([['mama'], ['papa']], cur.fetchall())
                self.assertEqual([], cur.fetchall())

    def test_nextset(self):
        with self.connect('identifier.sqlite') as c:
            with c.cursor() as cur:
                cur.execute('select ?, ?', ('mama', 'papa'))
                self.assertIsNone(cur.nextset())

    def test_describe(self):
        with self.connect('identifier.sqlite') as c:
            with c.cursor() as cur:
                cur.execute('select ? as m, ? as p', ('mama', 'papa'))
                self.assertIsNotNone(cur.description)
                self.assertEqual(['m', 'p'], [p[0] for p in cur.description])

    def test_commit2(self):
        with self.connect('identifier.sqlite') as c:
            c.commit()
            c.commit()

    def test_type(self):
        with self.connect('h2') as c:
            with c.cursor() as cur:
                cur.execute('select now()')
                self.assertIsInstance(cur.fetchone()[0], datetime.datetime)


if __name__ == '__main__':
    unittest.main()
