# encoding: utf-8

from containers import Datum

class BuilderMeta(type):
    def __new__(cls, name, bases, dict):
        dict.setdefault('__schema__', name)
        dict.setdefault('__id_fields__', ('id', ))
        return type.__new__(cls, name, bases, dict)

    def __call__(self, *args, **kwargs):
        field_values = dict(pair for pair in self.__dict__.items() if not pair[0].startswith('__'))
        field_values.update(kwargs)
        if self == Builder:
            raise TypeError("Only subclasses of Builder object is instantiable")
        return Datum(
            self.__dict__['__schema__'],
            self.__dict__['__id_fields__'],
            **field_values)

class Builder(object):
    __metaclass__ = BuilderMeta
