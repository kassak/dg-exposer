from intellij.alchemy.dgapi_connector import DGAPIConnector


class DynamicDialect_dgapi:
    @classmethod
    def get_dialect_cls(cls, url):
        base_dialect = cls.get_base_dialect(url)

        class Dynamic(DGAPIConnector, base_dialect):
            pass

        return Dynamic

    @classmethod
    def get_base_dialect(cls, url):
        dbms = DGAPIConnector.get_dbms(url)
        return _dialects.get(dbms, _default)()

    @classmethod
    def engine_created(cls, engine):
        pass


def _sqlite():
    import intellij.dgapi as dg
    dg.sqlite_version_info = (100500,)
    from sqlalchemy.dialects.sqlite.base import SQLiteDialect
    return SQLiteDialect


def _postgres():
    from sqlalchemy.dialects.postgresql.base import PGDialect
    return PGDialect


def _default():
    from sqlalchemy.engine import default
    return default.DefaultDialect


_dialects = {
    'SQLITE': _sqlite,
    'POSTGRES': _postgres
}
