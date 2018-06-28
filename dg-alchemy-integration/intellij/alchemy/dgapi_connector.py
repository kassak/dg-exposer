from sqlalchemy.connectors import Connector


class DGAPIConnector(Connector):
    driver = 'dgapi'

    supports_sane_rowcount_returning = False
    supports_sane_multi_rowcount = False

    supports_unicode_statements = True
    supports_unicode_binds = True

    supports_native_decimal = True
    default_paramstyle = 'qmark'

    def __init__(self, **kw):
        super(DGAPIConnector, self).__init__(**kw)

    @classmethod
    def dbapi(cls):
        import intellij.dgapi as module
        return module

    def create_connect_args(self, url):
        c_str = str(url)
        c_str = c_str[c_str.index("://") + 3:]
        return [[], {"dsn": c_str}]

    def is_disconnect(self, e, connection, cursor):
        if isinstance(e, self.dbapi.ProgrammingError):
            return "The cursor's connection has been closed." in str(e) or \
                   'Attempt to use a closed connection.' in str(e)
        else:
            return False
