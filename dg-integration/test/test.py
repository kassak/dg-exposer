import unittest
from intellij.dgapi import *
from intellij.discover import any_instance, create_instance

def run_test_app():
    trigger = 'TestApp is running on port: '
    import subprocess, os
    proc = subprocess.Popen(['gradle', 'test'], cwd=os.path.relpath('../dg-exposer', os.path.abspath(__file__)), stdout=subprocess.PIPE)
    for line in proc.stdout:
        line = line.decode("utf-8")
        idx = line.find(trigger)
        if idx != -1:
            port = int(line[idx + len(trigger):])
            print(trigger, port)
            return create_instance('127.0.0.1', port)

class TestDBAPI(unittest.TestCase):
    _test_instance = run_test_app()

    def test_simple(self):
        TestDBAPI._test_instance.noisy = True
        with connect(dsn='identifier.sqlite', inst=TestDBAPI._test_instance) as c:
            with c.cursor() as cur:
                cur.execute('create table a(a int)')
                c.commit()
                c.rollback()

    def test_fetch_one(self):
        TestDBAPI._test_instance.noisy = True
        with connect(dsn='identifier.sqlite', inst=TestDBAPI._test_instance) as c:
            with c.cursor() as cur:
                cur.execute('select ?, ?', ('mama', 'papa'))
                self.assertEqual(['mama', 'papa'], cur.fetchone())
                self.assertIsNone(cur.fetchone())

    def test_fetch_one_1(self):
        TestDBAPI._test_instance.noisy = True
        with connect(dsn='identifier.sqlite', inst=TestDBAPI._test_instance) as c:
            with c.cursor() as cur:
                cur.execute('select \'mama\', \'papa\'')
                self.assertEqual(['mama', 'papa'], cur.fetchone())
                self.assertIsNone(cur.fetchone())

    def test_fetch_one_2(self):
        TestDBAPI._test_instance.noisy = True
        with connect(dsn='identifier.sqlite', inst=TestDBAPI._test_instance) as c:
            with c.cursor() as cur:
                cur.execute('select ? union select ?', ('mama', 'papa'))
                self.assertEqual(['mama'], cur.fetchone())
                self.assertEqual(['papa'], cur.fetchone())
                self.assertIsNone(cur.fetchone())

    def test_fetch_all(self):
        TestDBAPI._test_instance.noisy = True
        with connect(dsn='identifier.sqlite', inst=TestDBAPI._test_instance) as c:
            with c.cursor() as cur:
                cur.execute('select ? union select ?', ('mama', 'papa'))
                self.assertEqual([['mama'], ['papa']], cur.fetchall())
                self.assertEqual([], cur.fetchall())

    def test_nextset(self):
        TestDBAPI._test_instance.noisy = True
        with connect(dsn='identifier.sqlite', inst=TestDBAPI._test_instance) as c:
            with c.cursor() as cur:
                cur.execute('select ?, ?', ('mama', 'papa'))
                self.assertIsNone(cur.nextset())

    def test_describe(self):
        TestDBAPI._test_instance.noisy = True
        with connect(dsn='identifier.sqlite', inst=TestDBAPI._test_instance) as c:
            with c.cursor() as cur:
                cur.execute('select ? as m, ? as p', ('mama', 'papa'))
                self.assertIsNotNone(cur.description)
                self.assertEqual(['m', 'p'], [p[0] for p in cur.description])

    def test_commit2(self):
        TestDBAPI._test_instance.noisy = True
        with connect(dsn='identifier.sqlite', inst=TestDBAPI._test_instance) as c:
            c.commit()
            c.commit()

    def test_type(self):
        TestDBAPI._test_instance.noisy = True
        with connect(dsn='pg', inst=TestDBAPI._test_instance) as c:
            with c.cursor() as cur:
                cur.execute('select now()')
                self.assertIsInstance(cur.fetchone()[0], datetime.datetime)


if __name__ == '__main__':
    unittest.main()
