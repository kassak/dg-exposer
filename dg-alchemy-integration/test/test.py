import unittest

from intellij.discover import any_instance
from sqlalchemy import create_engine

any_instance().noisy = True


class TestDialect(unittest.TestCase):
    def test_simple(self):
        engine = create_engine('dg://identifier.sqlite')
        with engine.connect() as c:
            res = c.execute("select 'mama' union select 'papa'")
            print(list(res))
            res.close()


if __name__ == '__main__':
    unittest.main()
