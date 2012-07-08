# encoding: utf-8

from types import FunctionType
from tableau.declarations import DynamicField, Aggregation, Lazy, one_to_many, many_to_one
from tableau.utils import _repr, is_iterable_container

def value_of(value):
    if isinstance(value, DynamicField):
        return value()
    else:
        return value

class DatumBase(object):
    _tableau_schema = None
    _tableau_id_fields = None
    _tableau_fields = None

    @property
    def _id(self):
        return tuple(getattr(self, k) for k in self._tableau_id_fields)

class Datum(DatumBase):
    def __init__(self, schema, id_fields, **fields):
        self._tableau_schema = schema
        if isinstance(id_fields, basestring):
            self._tableau_id_fields = (id_fields, )
        elif isinstance(id_fields, tuple):
            self._tableau_id_fields = id_fields
        else:
            self._tableau_id_fields = tuple(id_fields)
        self._tableau_fields = {}
        for k, v in fields.iteritems():
            setattr(self, k, v)

    def __setattr__(self, k, v):
        if k.startswith('_'):
            object.__setattr__(self, k, v)
        else:
            if isinstance(v, FunctionType):
                v = Lazy(v)
            elif isinstance(v, DatumBase):
                # implicit many_to_one
                v = many_to_one(v, k, v._tableau_id_fields)
            elif is_iterable_container(v):
                # implicit one_to_many
                v = one_to_many(v, k)
            if isinstance(v, DynamicField):
                v.bind(self, k)
            self._tableau_fields[k] = v

    def __getattribute__(self, k):
        if k.startswith('_'):
            return object.__getattribute__(self, k)
        else:
            try:
                return value_of(object.__getattribute__(self, '_tableau_fields')[k])
            except KeyError:
                raise AttributeError('%s.%s' % (self._tableau_schema, k))

    def __eq__(self, that):
        return id(self) == id(that)

    def __hash__(self):
        return object.__hash__(self)

    def __repr__(self):
        return 'Datum(%r, %r, %s)' % (self._tableau_schema, self._tableau_id_fields, ', '.join('%s=%s' % (pair[0], '...' if isinstance(pair[1], Aggregation) else _repr(value_of(pair[1]))) for pair in self._tableau_fields.iteritems()))
