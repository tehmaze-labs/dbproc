class Procedure(object):
    def __init__(self, backend, *args, **kwargs):
        self.backend = backend

    def __call__(self, *args, **kwargs):
        raise NotImplementedError
