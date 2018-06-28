from intellij.dgapi.exceptions import *
from intellij.dgapi.types import *

apilevel = "2.0"
threadsafety = 0
paramstyle = "qmark"


def connect(**kwargs):
    from intellij.dgapi.connection import Connection
    return Connection(**kwargs)
