import unittest
from intellij.dgapi import *
from intellij.discover import any_instance


class TestDBAPI(unittest.TestCase):
    def test_simple(self):
        inst = any_instance()
        inst.noisy = True
        with connect(dsn='identifier.sqlite', inst=inst) as c:
            with c.cursor() as cur:
                cur.execute('create table a(a int)')
                c.commit()

    def test_fetch_one(self):
        inst = any_instance()
        inst.noisy = True
        with connect(dsn='identifier.sqlite', inst=inst) as c:
            with c.cursor() as cur:
                cur.execute('select ?, ?', ('mama', 'papa'))
                self.assertEqual(['mama', 'papa'], cur.fetchone())
                self.assertIsNone(cur.fetchone())

    def test_fetch_one_2(self):
        inst = any_instance()
        inst.noisy = True
        with connect(dsn='identifier.sqlite', inst=inst) as c:
            with c.cursor() as cur:
                cur.execute('select ? union select ?', ('mama', 'papa'))
                self.assertEqual(['mama'], cur.fetchone())
                self.assertEqual(['papa'], cur.fetchone())
                self.assertIsNone(cur.fetchone())

    def test_fetch_all(self):
        inst = any_instance()
        inst.noisy = True
        with connect(dsn='identifier.sqlite', inst=inst) as c:
            with c.cursor() as cur:
                cur.execute('select ? union select ?', ('mama', 'papa'))
                self.assertEqual([['mama'], ['papa']], cur.fetchall())
                self.assertEqual([], cur.fetchall())

    def test_nextset(self):
        inst = any_instance()
        inst.noisy = True
        with connect(dsn='identifier.sqlite', inst=inst) as c:
            with c.cursor() as cur:
                cur.execute('select ?, ?', ('mama', 'papa'))
                self.assertIsNone(cur.nextset())



if __name__ == '__main__':
    unittest.main()
