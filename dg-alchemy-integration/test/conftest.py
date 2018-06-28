from sqlalchemy.dialects import registry

registry.register("access", "intellij.alchemy.dgapi", "AccessDialect_dgapi")
registry.register("access.dgapi", "intellij.alchemy.dgapi", "AccessDialect_dgapi")

from sqlalchemy.testing.plugin.pytestplugin import *