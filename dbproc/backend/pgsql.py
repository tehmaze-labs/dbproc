try:
    import psycopg2
    import psycopg2._psycopg
    import psycopg2.extras
except ImportError:
    psycopg2 = None

from dbproc.backend.base import Backend, Empty
from dbproc.procedure import Procedure


class PgSQLProc(Procedure):
    def __init__(self, backend, proc, param_count, param_names, param_types,
            return_type):
        self.backend = backend
        self.proc = proc
        self.param_count = param_count
        self.param_name = param_names
        self.param_type = param_types
        self.return_type = return_type

    def __call__(self, *args, **kwargs):
        query_args = list(args) + [Empty] * (self.param_count - len(args))

        for name, value in kwargs.iteritems():
            if name not in self.param_name:
                raise TypeError("%s() got an unexpected keyword argument "
                                "'%s'" % (self.proc, name))

            index = self.param_name.index(name)
            if query_args[index] is not Empty:
                raise TypeError("%s() got multiple values for keyword"
                                "argument '%s'" % (self.proc, name))
            else:
                query_args[index] = value

        if Empty in query_args:
            needs = self.param_count
            empty = len([x for x in query_args if x is Empty])
            raise TypeError("%s() takes exactly %d arguments (%d given)" %
                            (self.proc, needs, needs - empty))

        cursor = self.backend.get_cursor()
        cursor.callproc(self.proc, query_args)
        print self.proc, self.return_type
        try:
            if self.return_type == 'set':
                return cursor.fetchall()[self.proc]
            else:
                return cursor.fetchone()[self.proc]
        finally:
            cursor.close()


class PgSQLBackend(Backend):
    def __init__(self, *args, **kwargs):
        super(PgSQLBackend, self).__init__(*args, **kwargs)
        self.schema = self.schema or self.get_schema()
        self.inspect()

    @classmethod
    def can_handle(cls, instance):
        if psycopg2 is None:
            return False
        else:
            return isinstance(instance, psycopg2._psycopg.connection)

    def get_cursor(self):
        return self.connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor,
        )

    def get_schema(self):
        cursor = self.get_cursor()
        cursor.execute('SELECT current_schema() AS schema')
        try:
            return cursor.fetchone()['schema']
        finally:
            cursor.close()

    def inspect(self):
        query = '''
        SELECT
                p.proname AS proc
               ,p.pronargs AS param_count
               ,p.proargnames AS param_names
               ,string_to_array(
                array_to_string(p.proargtypes::regtype[], ' '), ' '
               ) AS param_types
               ,t.typname AS return_type
          FROM pg_catalog.pg_proc p
          JOIN pg_catalog.pg_namespace n
            ON n.oid = p.pronamespace
          JOIN pg_catalog.pg_type t
            ON t.oid = p.prorettype
         WHERE n.nspname = %s;
        '''
        cursor = self.get_cursor()
        cursor.execute(query, (self.schema,))
        for row in cursor.fetchall():
            proc = row['proc']
            print self.procedure
            self.procedure[proc] = PgSQLProc(self, **row)

        cursor.close()
