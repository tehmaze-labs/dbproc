try:
    import MySQLdb
    import MySQLdb.cursors
except ImportError:
    MySQLdb = None

import random
import string
from dbproc.backend.base import Backend, Empty
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
        cursor = self.backend.get_cursor()
        cursor.execute(query, args)
        try:
            return cursor.fetchone()['result']
        finally:
            cursor.close()


class MySQLProc(MySQLFunc):
    '''
    Wrapper for a MySQL stored procedure.
    '''

    parameter_characters = string.letters

    def __init__(self, *args, **kwargs):
        super(MySQLProc, self).__init__(*args, **kwargs)
        self.param_type = {}
        self.param_name = []
        self.inspect()

    def __call__(self, *args, **kwargs):
        cursor = self.backend.get_cursor()

        fetch = {}
        if self.param_name:
            param_seen = set([])
            query_args = [Empty] * len(self.param_name)

            # Process variadic arguments
            for i, arg in enumerate(args):
                query_args[i] = arg

            # Process keyword arguments
            for name, value in kwargs.iteritems():
                if name not in self.param_name:
                    raise TypeError("%s() got an unexpected keyword "
                                    "argument '%s'" % (self.proc, name))

                index = self.param_name.index(name)
                if query_args[index] is not Empty:
                    raise TypeError("%s() got multiple values for keyword "
                                    "argument '%s'" % (self.proc, name))
                else:
                    query_args[index] = value

                param_seen.add(name)

                # INOUT type parameters are a special case, an initial value
                # has to be provided and the parameter name is passed as a
                # procedure argument. MySQLdb automatically replaces INOUT and
                # OUT parameters by @_func_index place holder variables.
                if self.param_type[name] == 'inout':
                    fetch['@_%s_%d' % (self.proc, index)] = name

            # Check for left over OUT parameters, we want to catch those
            for name in param_seen ^ set(self.param_name):
                index = self.param_name.index(name)
                if self.param_type[name] == 'out':
                    query_args[index] = None
                    fetch['@_%s_%d' % (self.proc, index)] = name

        else:
            query_args = list(map(str, args))
            if kwargs:
                raise TypeError('MySQL procedure does not support named '
                                'arguments')

        if Empty in query_args:
            needs = len(query_args)
            empty = len([x for x in query_args if x is Empty])
            raise TypeError("%s() takes exactly %d argument (%d given)" %
                            (self.proc, needs, needs - empty))

        cursor.callproc(self.proc, query_args)

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
        Use the `mysql.proc` table to find out about this procedure's
        signature. This method requires `SELECT` privileges on the
        aformentioned table.
        '''
        query = 'SELECT * FROM mysql.proc WHERE db=%s AND specific_name=%s'
        cursor = self.backend.get_cursor()
        try:
            cursor.execute(query, (self.schema, self.proc))
        except MySQLdb.MySQLError, e:
            import warnings
            warnings.warn(str(e[1]))
            cursor.close()
            return

        signature = cursor.fetchone()
        # Comma separated values, really MySQL!?
        for param in signature['param_list'].split(', '):
            param_type, name, data_type = param.split()
            self.param_name.append(name)
            self.param_type[name] = param_type.lower()

        cursor.close()

    def generate_parameter(self, name):
        return '@%s' % '_'.join([
            name,
            ''.join([
                random.choice(self.parameter_characters)
                for x in xrange(8)
            ]),
        ])

class MySQLBackend(Backend):
    '''
    MySQL backend driver, supports both stored functions and stored procedure
    calls.

    Caveats
    =======

    The default `SHOW FUNCTION STATUS` and `SHOW PROCEDURE STATUS` queries do
    not show signatures, the actual information lives in the `mysql.proc`
    table, but this requires one to set up `SELECT` privileges.
    '''

    def __init__(self, *args, **kwargs):
        super(MySQLBackend, self).__init__(*args, **kwargs)
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
        '''
        Get the currently active schema, in MySQL schemas and databases are
        synonymous.
        '''
        cursor = self.get_cursor()
        cursor.execute('SELECT DATABASE() AS `schema`')
        try:
            return cursor.fetchone()['schema']
        finally:
            cursor.close()

    def inspect(self):
        query = 'SELECT * FROM information_schema.routines'
        cursor = self.get_cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
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

        cursor.close()
