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
                c.rollback()

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

    def test_describe(self):
        inst = any_instance()
        inst.noisy = True
        with connect(dsn='identifier.sqlite', inst=inst) as c:
            with c.cursor() as cur:
                cur.execute('select ? as m, ? as p', ('mama', 'papa'))
                self.assertIsNotNone(cur.description)
                self.assertEqual(['m', 'p'], [p[0] for p in cur.description])

    def test_commit2(self):
        inst = any_instance()
        inst.noisy = True
        with connect(dsn='identifier.sqlite', inst=inst) as c:
            c.commit()
            c.commit()

    def test_type(self):
        inst = any_instance()
        inst.noisy = True
        with connect(dsn='pg', inst=inst) as c:
            with c.cursor() as cur:
                cur.execute('select now()')
                self.assertIsInstance(cur.fetchone()[0], datetime.datetime)



if __name__ == '__main__':
    unittest.main()
