from dbproc.backend import mysql, pgsql
from dbproc.backend.base import Backend


class Wrap(object):
    '''
    Provide a stored procedure wrapper for the given `connection`, optionally
    you may provide an alternate `schema`. You can also limit the callable
    procedures by supplying a `prefix`.

    :param connection: instance of DB API 2.0 connection
    :param schema: name of the schema
    :param prefix: name prefix
    :rtype: instance of :class:`dbproc.backend.base.Backend`

    >>> wrapped = Wrap(connection)
    >>> wrapped.test(...)
    ...
    '''
    def __init__(self, connection, schema=None, prefix=''):
        self.backend = Backend.for_connection(connection,
                                              schema=schema)
        self.prefix = prefix

    def __getattr__(self, attr):
        if attr.startswith('_'):
            return super(Wrap, self).__getattr__(self, attr)
        else:
            proc = ''.join([self.prefix, attr])
            try:
                return self.backend[proc]
            except KeyError:
                raise AttributeError('No stored function/procedure called %s' % attr)
