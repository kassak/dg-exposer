from sqlalchemy.dialects import registry

registry.register("dg", "intellij.alchemy.dgapi", "DynamicDialect_dgapi")

from sqlalchemy.testing.plugin.pytestplugin import *