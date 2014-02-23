CLASSES = {}


class BackendTracker(type):
    def __new__(meta, name, bases, attrs):
        cls = type.__new__(meta, name, bases, attrs)
        if name != 'Backend':
            CLASSES[name] = cls
        return cls


class Backend(object):
    '''
    Base class for supported backends.
    '''

    __metaclass__ = BackendTracker

    def __init__(self, connection, schema=None, prefix=''):
        self.connection = connection
        self.schema = schema
        self.prefix = prefix
        self.procedure = {}

    @classmethod
    def can_handle(self, instance):
        '''
        Check if this backend can handle a connection for `instance`.

        :param instance: a DB API 2.0 connection instance
        :rtype: bool
        '''
        raise NotImplementedError

    @classmethod
    def for_connection(self, instance, *args, **kwargs):
        '''
        Returns a backend class if the `instance` connection is supported.

        :param instance: a DB API 2.0 connection instance
        :rtype: :class:`Backend` subclass
        '''
        for cls in CLASSES.values():
            if cls.can_handle(instance):
                return cls(instance, *args, **kwargs)

        raise TypeError('Connection type %r not supported' % instance)

    def __contains__(self, func):
        '''
        Check if the procedure with the name `func` is available.

        :param func: str
        :rtype: bool
        '''
        return func in self.procedure

    def __getitem__(self, func):
        '''
        Returns a callable object for the procedure with the name `func`.

        :param func: str
        :rtype: callable
        '''
        if func in self.procedure:
            return self.procedure[func]
        else:
            raise KeyError(func)


class Empty(type):
    '''
    Placeholder class.
    '''
    pass
