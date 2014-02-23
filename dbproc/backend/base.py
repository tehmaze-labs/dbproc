CLASSES = {}


class BackendTracker(type):
    def __new__(meta, name, bases, attrs):
        cls = type.__new__(meta, name, bases, attrs)
        if name != 'Backend':
            CLASSES[name] = cls
        return cls


class Backend(object):
    __metaclass__ = BackendTracker

    def __init__(self, connection, schema=None, prefix=''):
        self.connection = connection
        self.schema = schema
        self.prefix = prefix
        self.procedure = {}

    @classmethod
    def can_handle(self, instance):
        raise NotImplementedError

    @classmethod
    def for_connection(self, instance, *args, **kwargs):
        for cls in CLASSES.values():
            if cls.can_handle(instance):
                return cls(instance, *args, **kwargs)

        raise TypeError('Connection type %r not supported' % instance)

    def __contains__(self, func):
        return func in self.procedure

    def __getitem__(self, func):
        if func in self.procedure:
            return self.procedure[func]
        else:
            raise KeyError(func)


class Empty(type):
    '''
    Placeholder class.
    '''
    pass
