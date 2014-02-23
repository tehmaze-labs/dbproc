try:
    import psycopg2
except ImportError:
    psycopg2 = None

from dbproc.backend.base import Backend


class PgSQLBackend(Backend):
    def __init__(self, *args, **kwargs):
        super(PgSQLBackend, self).__init__(*args, **kwargs)
        self.schema = self.schema or 'public'

    @classmethod
    def can_handle(cls, instance):
        if psycopg2 is None:
            return False
        else:
            return isinstance(instance, psycopg2.connection)
