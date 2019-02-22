from sqlalchemy.dialects import registry

registry.register("dg", "intellij.alchemy.dgapi", "DefaultDialect_dgapi")

from sqlalchemy.testing.plugin.pytestplugin import *