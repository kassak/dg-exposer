from sqlalchemy.testing.requirements import SuiteRequirements

from sqlalchemy.testing import exclusions

class Requirements(SuiteRequirements):
    def __init__(self):
        pass

    @property
    def order_by_col_from_union(self):
        return exclusions.closed()

    @property
    def cross_schema_fk_reflection(self):
        return exclusions.closed()

    @property
    def table_reflection(self):
        return exclusions.closed()

    @property
    def returning(self):
        return exclusions.open()