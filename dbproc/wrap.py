from dbproc.backend import mysql, pgsql
from dbproc.backend.base import Backend


class Wrap(object):
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
