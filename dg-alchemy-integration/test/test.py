import unittest

__import__('intellij.alchemy')
from sqlalchemy import create_engine
from intellij.discover import any_instance

any_instance().noisy = True

class TestDialect(unittest.TestCase):
    def test_simple(self):
        engine = create_engine('default+dgapi://as/q?dsn=identifier.sqlite')
        with engine.connect() as c:
            res = c.execute("select 'mama' union select 'papa'")
            print(list(res))
            res.close()

if __name__ == '__main__':
    unittest.main()
