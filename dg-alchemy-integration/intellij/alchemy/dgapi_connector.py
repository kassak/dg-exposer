from sqlalchemy.connectors import Connector

class DGAPIConnector(Connector):
    driver = 'dgapi'

    supports_sane_rowcount_returning = False
    supports_sane_multi_rowcount = False

    supports_unicode_statements = True
    supports_unicode_binds = True

    supports_native_decimal = True
    default_paramstyle = 'named'

    def __init__(self, **kw):
        super(DGAPIConnector, self).__init__(**kw)

    @classmethod
    def dbapi(cls):
        import intellij.dgapi as module
        return module

    def create_connect_args(self, url):
        #opts = url.translate_connect_args(username='user')
        # opts.update(url.query)
        #
        # keys = opts
        #
        # query = url.query
        #
        # connect_args = {}
        # for param in ('ansi', 'unicode_results', 'autocommit'):
        #     if param in keys:
        #         connect_args[param] = util.asbool(keys.pop(param))
        #
        # if 'odbc_connect' in keys:
        #     connectors = [util.unquote_plus(keys.pop('odbc_connect'))]
        # else:
        #     def check_quote(token):
        #         if ";" in str(token):
        #             token = "'%s'" % token
        #         return token
        #
        #     keys = dict(
        #         (k, check_quote(v)) for k, v in keys.items()
        #     )
        #
        #     dsn_connection = 'dsn' in keys or \
        #         ('host' in keys and 'database' not in keys)
        #     if dsn_connection:
        #         connectors = ['dsn=%s' % (keys.pop('host', '') or
        #                                   keys.pop('dsn', ''))]
        #     else:
        #         port = ''
        #         if 'port' in keys and 'port' not in query:
        #             port = ',%d' % int(keys.pop('port'))
        #
        #         connectors = []
        #         driver = keys.pop('driver', self.pyodbc_driver_name)
        #         if driver is None:
        #             util.warn(
        #                 "No driver name specified; "
        #                 "this is expected by PyODBC when using "
        #                 "DSN-less connections")
        #         else:
        #             connectors.append("DRIVER={%s}" % driver)
        #
        #         connectors.extend(
        #             [
        #                 'Server=%s%s' % (keys.pop('host', ''), port),
        #                 'Database=%s' % keys.pop('database', '')
        #             ])
        #
        #     user = keys.pop("user", None)
        #     if user:
        #         connectors.append("UID=%s" % user)
        #         connectors.append("PWD=%s" % keys.pop('password', ''))
        #     else:
        #         connectors.append("Trusted_Connection=Yes")
        #
        #     # if set to 'Yes', the ODBC layer will try to automagically
        #     # convert textual data from your database encoding to your
        #     # client encoding.  This should obviously be set to 'No' if
        #     # you query a cp1253 encoded database from a latin1 client...
        #     if 'odbc_autotranslate' in keys:
        #         connectors.append("AutoTranslate=%s" %
        #                           keys.pop("odbc_autotranslate"))
        #
        #     connectors.extend(['%s=%s' % (k, v) for k, v in keys.items()])

        return [[], url.query] #[[";".join(connectors)], connect_args]

    def is_disconnect(self, e, connection, cursor):
        if isinstance(e, self.dbapi.ProgrammingError):
            return "The cursor's connection has been closed." in str(e) or \
                'Attempt to use a closed connection.' in str(e)
        else:
            return False

