# encoding: utf-8

from types import FunctionType
from tableau.declarations import DynamicField, Aggregation, Lazy, one_to_many, many_to_one
from tableau.utils import _repr, is_iterable_container

class Datum(object):
    def __init__(self, schema, id_fields, **fields):
        self._schema = schema
        if isinstance(id_fields, basestring):
            self._id_fields = (id_fields, )
        elif isinstance(id_fields, tuple):
            self._id_fields = id_fields
        else:
            self._id_fields = tuple(id_fields)
        self._fields = {}
        for k, v in fields.iteritems():
            setattr(self, k, v)

    @property
    def _id(self):
        return tuple(getattr(self, k) for k in self._id_fields)

    def __setattr__(self, k, v):
        if k.startswith('_'):
            object.__setattr__(self, k, v)
        else:
            if isinstance(v, FunctionType):
                v = Lazy(v)
            elif isinstance(v, Datum):
                # implicit many_to_one
                v = many_to_one(v, k, v._id_fields)
            elif is_iterable_container(v):
                # implicit one_to_many
                v = one_to_many(v, k)
            if isinstance(v, DynamicField):
                v.bind(self, k)
            self._fields[k] = v

    def __getattr__(self, k):
        try:
            return Datum.__value_of(object.__getattribute__(self, '_fields')[k])
        except KeyError:
            raise
            raise AttributeError('%s.%s' % (self._schema, k))

    def __eq__(self, that):
        return id(self) == id(that)

    def __hash__(self):
        return object.__hash__(self)

    @staticmethod
    def __value_of(value):
        if isinstance(value, DynamicField):
            return value()
        else:
            return value

    def __repr__(self):
        return 'Datum(%r, %r, %s)' % (self._schema, self._id_fields, ', '.join('%s=%s' % (pair[0], '...' if isinstance(pair[1], Aggregation) else _repr(Datum.__value_of(pair[1]))) for pair in self._fields.iteritems()))
