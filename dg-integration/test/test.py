import unittest
from intellij.dgapi import *
from intellij.discover import any_instance


class TestDBAPI(unittest.TestCase):
    def test(self):
        inst = any_instance()
        inst.noisy = True
        with connect(dsn='identifier.sqlite', inst=inst) as c:
            with c.cursor() as cur:
                cur.execute('create table a(a int)')
                c.commit()


if __name__ == '__main__':
    unittest.main()
