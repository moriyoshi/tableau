from tableau.utils import is_iterable_container, string_container_from_value, _repr
from inspect import getargspec

__all__ = [
    'DynamicField',
    'Aggregation',
    'many_to_one',
    'many_to_many',
    'one_to_many',
    'auto',
    'unspecified',
    ]

class UnspecifiedType(object):
    pass

unspecified = UnspecifiedType()

class DynamicField(object):
    rendered = False

    def __init__(self):
        pass

    def bind(self, container, name):
        pass 

    def render(self):
        return self()

    def __call__(self):
        pass

class Lazy(DynamicField):
    rendered = True

    def __init__(self, func):
        self.func = func
        self.container = None
        self.name = None
        self.argspec = getargspec(self.func)

    def bind(self, container, name):
        self.container = container
        self.name = name

    def __call__(self):
        if self.argspec.varargs is not None:
            return self.func(*(self.container, self.name))
        elif self.argspec.keywords is not None:
            return self.func(**dict(container=self.container, name=self.name))
        else:
            return self.func(*(self.container, self.name)[0:len(self.argspec.args)])

class many_to_one(DynamicField):
    def __init__(self, schema_or_value=unspecified, this_side_fields=None, other_side_fields=None):
        self.rendered = False
        from tableau.containers import DatumBase
        self._value = None
        self._value_set = False
        if isinstance(schema_or_value, DatumBase):
            self.schema = schema_or_value._tableau_schema
            self.value = schema_or_value
        elif schema_or_value is None:
            self.schema = schema_or_value
            self.value = None
        elif isinstance(schema_or_value, basestring):
            self.schema = schema_or_value
        elif schema_or_value is not unspecified:
            raise TypeError("schema_or_value must be a DatumBase, None, or Unspecified")

        self.this_side_fields = string_container_from_value(this_side_fields, tuple)
        self.other_side_fields = string_container_from_value(other_side_fields, tuple)

    def get_value(self):
        if not self._value_set:
            raise ValueError("value is not specified")
        return self._value

    def set_value(self, value):
        self._value = value
        self._value_set = True

    value=property(get_value, set_value)

    def bind(self, container, name):
        if self.this_side_fields is None:
            self.this_side_fields = (name, )
            self.rendered = True

    def render(self):
        if self.value is None:
            return None
        other_side_fields = self.other_side_fields or self.value._tableau_id_fields
        if len(other_side_fields) > 1:
            raise ValueError("multiple identifiers cannot be rendered to a single field")
        return getattr(self.value, other_side_fields[0])

    def __call__(self):
        return self.value

    def __repr__(self):
        return 'many_to_one(schema=%s, this_side_fields=%s, other_side_fields=%s)' % (self.schema, _repr(self.this_side_fields), _repr(self.other_side_fields))

class Aggregation(object):
    pass

class many_to_many(DynamicField, Aggregation):
    def __init__(self, collection, this_side_fields, other_side_fields, via=None):
        self.collection = collection
        self.this_side_fields = string_container_from_value(this_side_fields, tuple)
        self.other_side_fields = string_container_from_value(other_side_fields, tuple)
        self.via = via

    def __call__(self):
        return self.collection

    def __repr__(self):
        return 'many_to_many(..., this_side_fields=%s, other_side_fields=%s, via=%s)' % (_repr(self.this_side_fields), _repr(self.other_side_fields), _repr(self.via))

class one_to_many(DynamicField, Aggregation):
    def __init__(self, collection, referring_fields=None, referred_fields=None):
        self.collection = collection
        self.referring_fields = string_container_from_value(referring_fields, tuple)
        self.referred_fields = string_container_from_value(referred_fields, tuple)

    def __call__(self):
        return self.collection

    def __repr__(self):
        return 'one_to_many(..., referring_fields=%s, referred_fields=%s)' % (_repr(self.referring_fields), _repr(self.referred_fields))

class auto(tuple):
    def __new__(self, args):
        return tuple.__new__(self, (args, ))
