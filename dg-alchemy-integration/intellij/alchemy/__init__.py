from sqlalchemy.dialects import registry

registry.register("default", "intellij.alchemy.dgapi", "DefaultDialect_dgapi")
registry.register("default.dgapi", "intellij.alchemy.dgapi", "DefaultDialect_dgapi")
