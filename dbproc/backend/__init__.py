from dbproc.backend.base import Backend


def for_connection(instance):
    for cls in Backend.classes.values():
        if cls.can_handle(instance):
            return cls(instance)
