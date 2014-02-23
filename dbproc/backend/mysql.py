try:
    import MySQLdb
    import MySQLdb.cursors
except ImportError:
    MySQLdb = None

import random
import string
from dbproc.backend.base import Backend
from dbproc.procedure import Procedure


class MySQLFunc(Procedure):
    '''
    Wrapper for a MySQL stored function.
    '''

    def __init__(self, backend, proc, schema=None, data_type=None):
        self.backend = backend
        self.proc = proc
        self.schema = schema or self.backend.schema
        self.data_type = data_type

    def __call__(self, *args, **kwargs):
        if kwargs:
            raise TypeError('MySQL function does not support named arguments')

        query_args = ['%s'] * len(args)
        query = 'SELECT %(schema)s.%(proc)s(%(args)s) AS result' % dict(
            schema=self.schema,
            args=', '.join(query_args),
            proc=self.proc,
        )
        self.backend.cursor.execute(query, args)
        return self.backend.cursor.fetchone()['result']


class MySQLProc(MySQLFunc):
    '''
    Wrapper for a MySQL stored procedure.
    '''

    parameter_characters = string.letters

    def __init__(self, *args, **kwargs):
        super(MySQLProc, self).__init__(*args, **kwargs)
        self.param_type = {'in': set([]), 'out': set([]), 'inout': set([])}
        self.param_name = []
        self.inspect()

    def __call__(self, *args, **kwargs):
        cursor = self.backend.get_cursor()

        query_vars = ['%s'] * len(args)
        query_args = list(map(str, args))
        quote_args = [self.backend.connection.escape_string] * len(args)
        fetch = {}
        if kwargs:
            if not self.param_name:
                raise TypeError('MySQL procedure does not support named arguments')

            query_vars = ['%s'] * len(self.param_name)
            query_args += [None] * (len(self.param_name) - len(query_args))
            quote_args = [self.backend.connection.escape_string] * len(query_args)
            for name, value in kwargs.iteritems():
                if name not in self.param_name:
                    raise TypeError("%s() got an unexpected keyword "
                                    "argument '%s'" % (self.proc, name))

                index = self.param_name.index(name)
                query_args[index] = str(value)
                if name in self.param_type['inout']:
                    pname = self.generate_parameter(name)
                    query = 'SET @%s = %%s' % (pname,)
                    cursor.execute(query, (value,))
                    query_args[index] = '@%s' % pname
                    quote_args[index] = lambda s: s
                    fetch['@%s' % pname] = name

        query = 'CALL %(schema)s.%(proc)s(%(args)s)' % dict(
            schema=self.schema,
            args=', '.join(query_vars),
            proc=self.proc,
        )
        query = query % tuple([quote_args[x](query_args[x])
                               for x in xrange(0, len(query_args))])
        cursor.execute(query)

        try:
            if fetch:
                cursor.execute('SELECT %s' % ', '.join(fetch.keys()))
                result = cursor.fetchone()
                for pname, name in fetch.iteritems():
                    result[name] = result.pop(pname)
                return result
            else:
                return cursor.fetchall()
        finally:
            cursor.close()

    def inspect(self):
        '''
        Use the `mysql.proc` table to find out about this procedure's signature.
        '''
        query = 'SELECT * FROM mysql.proc WHERE db=%s AND specific_name=%s'
        try:
            self.backend.cursor.execute(query, (self.schema, self.proc))
        except MySQLdb.MySQLError, e:
            import warnings
            warnings.warn(str(e[1]))
            return

        signature = self.backend.cursor.fetchone()
        for param in signature['param_list'].split(', '):
            direction, name, data_type = param.split()
            self.param_type[direction.lower()].add(name)
            self.param_name.append(name)

    def generate_parameter(self, name):
        return '_'.join([
            name,
            ''.join([
                random.choice(self.parameter_characters)
                for x in xrange(8)
            ]),
        ])

class MySQLBackend(Backend):
    def __init__(self, *args, **kwargs):
        super(MySQLBackend, self).__init__(*args, **kwargs)
        self.cursor = self.get_cursor()
        self.schema = self.schema or self.get_schema()
        self.inspect()

    @classmethod
    def can_handle(cls, instance):
        if MySQLdb is None:
            return False
        else:
            return isinstance(instance, MySQLdb.connection)

    def get_cursor(self):
        return self.connection.cursor(MySQLdb.cursors.DictCursor)

    def get_schema(self):
        query = 'SELECT DATABASE() AS `schema`'
        self.cursor.execute(query)
        return self.cursor.fetchone()['schema']

    def inspect(self):
        query = 'SELECT * FROM information_schema.routines'
        self.cursor.execute(query)
        for row in self.cursor.fetchall():
            proc = row['ROUTINE_NAME']
            schema = row['ROUTINE_SCHEMA']
            if row['ROUTINE_TYPE'] == 'FUNCTION':
                self.procedure[proc] = MySQLFunc(self,
                                                 proc=proc,
                                                 schema=schema,
                                                 data_type=row['DATA_TYPE'])
            elif row['ROUTINE_TYPE'] == 'PROCEDURE':
                self.procedure[proc] = MySQLProc(self,
                                                 proc=proc,
                                                 schema=schema)

    def __contains__(self, func):
        return func in self.procedure

    def __getitem__(self, func):
        if func in self.procedure:
            return self.procedure[func]
        else:
            raise KeyError(func)
