from sqlalchemy.schema import Table
from tableau.containers import DatumBase
from tableau.declarations import DynamicField, Lazy, one_to_many, many_to_one, many_to_many, auto
from tableau.utils import string_container_from_value, is_iterable_container, is_callable
from sqlalchemy.orm import mapper, relationship
from sqlalchemy.orm.mapper import Mapper
from sqlalchemy.orm.properties import RelationshipProperty
from sqlalchemy.orm.attributes import InstrumentedAttribute, manager_of_class
from sqlalchemy.orm.instrumentation import unregister_class, instrumentation_registry
from types import FunctionType
from warnings import warn
from weakref import WeakKeyDictionary

def newSADatum(metadata):
    table_to_managed_class = WeakKeyDictionary()
    for class_, manager_getter in instrumentation_registry._manager_finders.iteritems():
        table_to_managed_class.setdefault(manager_getter(class_).mapper.local_table, []).append(class_)

    def default_value(column_def):
        if column_def.default is None:
            return None
        if not column_def.default.is_scalar:
            warn("non-scalar default value is not supported")
            return None
        return column_def.default.arg

    mixin_class_registry = WeakKeyDictionary()

    def clone_mapper(mapper_, class_):
        def clone_property(prop):
            if isinstance(prop, RelationshipProperty):
                if is_callable(prop.argument):
                    argument_ = lambda: lookup_mixin_class(prop.argument())
                else:
                    argument_ = lookup_mixin_class(prop.argument)
                return relationship(
                    argument_,
                    secondary=prop.secondary,
                    primaryjoin=prop.primaryjoin,
                    secondaryjoin=prop.secondaryjoin,
                    foreign_keys=prop._user_defined_foreign_keys,
                    uselist=prop.uselist,
                    order_by=prop.order_by,
                    backref=prop.backref,
                    back_populates=prop.back_populates,
                    post_update=prop.post_update,
                    cascade=",".join(prop.cascade),
                    extension=prop.extension,
                    viewonly=prop.viewonly,
                    lazy=prop.lazy,
                    collection_class=prop.collection_class,
                    passive_deletes=prop.passive_deletes,
                    passive_updates=prop.passive_updates,
                    remote_side=prop.remote_side,
                    enable_typechecks=prop.enable_typechecks,
                    join_depth=prop.join_depth,
                    comparator_factory=prop.comparator_factory,
                    single_parent=prop.single_parent,
                    innerjoin=prop.innerjoin,
                    doc=prop.doc,
                    active_history=prop.active_history,
                    cascade_backrefs=prop.cascade_backrefs,
                    load_on_pending=prop.load_on_pending,
                    strategy_class=prop.strategy_class,
                    query_class=prop.query_class
                    )
            else:
                return prop

        return mapper(
            class_=class_,
            local_table=mapper_.local_table,
            properties=dict((prop.key, clone_property(prop)) for prop in mapper_.iterate_properties),
            primary_key=mapper_.primary_key,
            non_primary=False,
            inherits=(mapper_.inherits and lookup_mixin_class(mapper_.inherits.class_)),
            inherit_condition=mapper_.inherit_condition,
            inherit_foreign_keys=mapper_.inherit_foreign_keys,
            order_by=mapper_.order_by,
            always_refresh=mapper_.always_refresh,
            version_id_col=mapper_.version_id_col,
            version_id_generator=mapper_.version_id_generator,
            polymorphic_on=mapper_.polymorphic_on,
            polymorphic_identity=mapper_.polymorphic_identity,
            concrete=mapper_.concrete,
            with_polymorphic=mapper_.with_polymorphic,
            allow_partial_pks=mapper_.allow_partial_pks,
            batch=mapper_.batch,
            column_prefix=mapper_.column_prefix,
            include_properties=mapper_.include_properties,
            exclude_properties=mapper_.exclude_properties,
            passive_updates=mapper_.passive_updates,
            eager_defaults=mapper_.eager_defaults
            )

    def lookup_mixin_class(managed_class):
        class_name = "SADatum#%s" % managed_class.__name__
        retval = mixin_class_registry.get(managed_class)
        if retval is None:
            mapper = manager_of_class(managed_class).mapper
            dict_ = dict(pair for pair in managed_class.__dict__.items()
                         if not isinstance(pair[1], InstrumentedAttribute) and pair[0] != '__init__' and not pair[0].startswith('_sa_'))
            dict_['_tableau_managed_class'] = managed_class
            if mapper.inherits:
                super_ = lookup_mixin_class(mapper.inherits.class_)
            else:
                super_ = SADatum
            retval = type(class_name, (super_, ), dict_)
            retval.__mapper__ = clone_mapper(mapper, retval)
            mixin_class_registry[managed_class] = retval
        return retval

    def managed_class_of_table(table):
        classes = table_to_managed_class.get(table)
        if classes is None:
            return None
        if len(classes) > 1:
            raise TypeError("More than one managed class (%s) registered for table %s" % (", ".join(class_.__name__ for class_ in classes), table))
        return classes[0]

    class SADatum(DatumBase):
        _tableau_managed_class = None
        _tableau_table = None

        @staticmethod
        def cleanup():
            for managed_class, sadatum_class in mixin_class_registry.items():
                unregister_class(sadatum_class)

        def __new__(cls, schema, id_fields=None, **fields):
            if isinstance(schema, basestring):
                table = metadata.tables.get(schema, None)
                if table is None:
                    raise ValueError("%s is not defined in the metadata" % schema)
                managed_class = managed_class_of_table(table)
            elif isinstance(schema, Table):
                table = schema
                managed_class = managed_class_of_table(table)
            elif isinstance(schema, Mapper):
                table = schema.local_table
                managed_class = schema.class_
            else:
                manager = manager_of_class(schema)
                if manager is not None:
                    managed_class = manager
                    table = manager.mapper.local_table
                else:
                    raise TypeError("schema must be either a table name or a %s instance" % Table.__name__)

            if managed_class is not None:
                assert not issubclass(managed_class, DatumBase)
                _cls = lookup_mixin_class(managed_class)
            else:
                _cls = cls
            newinstance = object.__new__(_cls)
            newinstance._tableau_table = table
            return newinstance

        def __init__(self, schema, id_fields=None, **fields):
            columns = self._tableau_table.columns

            _fields = dict((k, default_value(columns[k])) for k in columns.keys())
            _fields.update(fields)
            primary_key_columns = self._tableau_table.primary_key.columns.keys()
            if id_fields is not None:
                if isinstance(id_fields, basestring):
                    id_fields = (id_fields, )
                elif isinstance(id_fields, tuple):
                    id_fields = id_fields
                else:
                    id_fields = tuple(id_fields)
                if len(primary_key_columns) != len(id_fields):
                    id_fields_matched = False
                else:
                    id_fields_matched = True
                    for k1, k2 in zip(primary_key_columns, id_fields):
                        if k1 != k2:
                            id_fields_matched = False
                            break
                if not id_fields_matched:
                    raise ValueError('id_fields does not match to the table definition ([%s] != [%s])' % (','.join(id_fields), ','.join(primary_key_columns)))
            else:
                if len(primary_key_columns) == 1 and \
                    self._tableau_table.primary_key.columns[primary_key_columns[0]].autoincrement:
                    id_fields = auto(primary_key_columns[0])
                else:
                    id_fields = tuple(primary_key_columns)
            self._tableau_schema = self._tableau_table.name
            self._tableau_id_fields = id_fields
            self._tableau_fields = {}
            for k, v in _fields.iteritems():
                setattr(self, k, v)

        def __check_key_is_declared(self, k):
            if k not in self._tableau_table.columns and hasattr(self, '__mapper__') and not self.__mapper__.has_property(k):
                raise KeyError("%s is not declared in the table definition or mapper configuration" % k)

        def _value_of(self, k, value):
            if isinstance(value, one_to_many):
                prop = self.__mapper__.get_property(k) 
                if prop.uselist:
                    return value()
                else:
                    return value()[0]
            elif isinstance(value, DynamicField):
                return value()
            else:
                return value

        def __getattribute__(self, k):
            if k.startswith('_') or hasattr(self.__class__, k):
                return object.__getattribute__(self, k)
            try:
                return object.__getattribute__(self, '_tableau_fields')[k]
            except KeyError:
                raise AttributeError('%s.%s' % (self._tableau_schema, k))
                
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

                if isinstance(v, many_to_one):
                    for _k in v.this_side_fields:
                        self.__check_key_is_declared(_k)
                elif isinstance(v, one_to_many):
                    self.__check_key_is_declared(k)
                    if v.referred_fields is not None:
                        for _k in v.referred_fields:
                            self.__check_key_is_declared(_k)
                object.__getattribute__(self, '_tableau_fields')[k] = v
                if self._tableau_managed_class is not None and not isinstance(v, Lazy):
                    object.__setattr__(self, k, self._value_of(k, v))

        def _tableau_on_fixation(self):
            for k, v in self._tableau_fields.items():
                if self._tableau_managed_class is not None and isinstance(v, Lazy):
                    object.__setattr__(self, k, self._value_of(k, v))

    return SADatum
