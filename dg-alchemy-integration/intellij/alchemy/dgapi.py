from sqlalchemy.engine import default

from intellij.alchemy.dgapi_connector import DGAPIConnector


class DefaultDialect_dgapi(DGAPIConnector, default.DefaultDialect):
    pass

