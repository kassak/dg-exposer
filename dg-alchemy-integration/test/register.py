from sqlalchemy.dialects import registry


def register():
    registry.register("dg", "intellij.alchemy.dgapi", "DefaultDialect_dgapi")


