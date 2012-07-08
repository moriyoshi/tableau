# encoding: utf-8

import operator
import logging
import re
from warnings import warn
from itertools import chain
from tableau.containers import Datum
from tableau.declarations import one_to_many, many_to_many, many_to_one, DynamicField, auto

__all__ = [
    'DataSet',
    'ReferenceGraph',
    'DataSuite',
    'DataWalker',
    ]

class DataSet(object):
    logger = logging.getLogger('tableau.DataSet')

    def __init__(self, schema):
        self.schema = schema
        self.data = set()
        self.seq = 1

    def add(self, datum):
        if datum in self.data:
            return False

        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug('Trying to add %s' % datum)

        if isinstance(datum._tableau_id_fields, auto):
            setattr(datum, datum._tableau_id_fields[0], self.seq)
            assert getattr(datum, datum._tableau_id_fields[0], self.seq)
            self.seq += 1
        self.data.add(datum)
        return True

    def get(self):
        return sorted(iter(self.data),
            lambda a, b: \
                cmp(getattr(a, a._tableau_id_fields[0]), getattr(b, b._tableau_id_fields[0])) \
                if a._tableau_id_fields and b._tableau_id_fields else 0)

    def __iter__(self):
        return iter(self.get())
 
class ReferenceGraph(object):
    def __init__(self):
        self.references = {}
        self.back_references = {}
        self.weight_cache = None

    def add_reference(self, referencing, referenced):
        references = self.references.setdefault(referencing, set())
        if referenced is not None:
            references.add(referenced)
            self.back_references.setdefault(referenced, set()).add(referencing)
        self.weight_cache = None

    def get_weight(self, referenced):
        return self._get_weight(referenced, set())

    def _get_weight(self, referenced, seen):
        if self.weight_cache is not None:
            weight = self.weight_cache.get(referenced)
            if weight is not None:
                return weight
        if referenced in seen:
            return 0
        seen.add(referenced)
        weight = 1
        back_references = self.back_references.get(referenced)
        if back_references is not None:
            for referencing in back_references:
                weight += self._get_weight(referencing, seen)
        if self.weight_cache is None:
            self.weight_cache = {}
        self.weight_cache[referenced] = weight
        return weight

    def getlist(self):
        return sorted(self.references.iterkeys(),
                lambda a, b: \
                    cmp(self.get_weight(a),
                         self.get_weight(b)))

class DataSuite(object):
    def __init__(self):
        self.datasets = {}
        self.digraph = ReferenceGraph()

    def add_dependency(self, depending, depended_on):
        self.digraph.add_reference(depending, depended_on)

    def __getitem__(self, schema):
        dataset = self.datasets.get(schema)
        if dataset is None:
            self.digraph.add_reference(schema, None)
            dataset = DataSet(schema)
            self.datasets[schema] = dataset
        return dataset

    def __iter__(self):
        for schema in reversed(self.digraph.getlist()):
            yield self.datasets[schema]

class DataWalker(object):
    logger = logging.getLogger('tableau.DataWalker')

    def __init__(self, suite):
        self.suite = suite

    def _handle_one_to_many(self, datum, name, value):
        for _datum in iter(value()):
            self.suite.add_dependency(_datum._tableau_schema, datum._tableau_schema)
            referring_fields = value.referring_fields
            referred_fields = value.referred_fields
            m = {}
            if referring_fields is not None:
                if referred_fields is not None:
                    if len(referring_fields) != len(referred_fields):
                        raise ValueError("%s.%s: len(referring_fields) != len(referred_fields) (%d != %d)" % (datum._tableau_schema, name, len(referring_fields), len(referred_fields)))
                else:
                    if len(referring_fields) != len(datum._tableau_id_fields):
                        raise ValueError("%s.%s: len(referring_fields) != len(_tableau_id_fields) (%d != %d)" % (datum._tableau_schema, name, len(referring_fields), len(datum._tableau_id_fields)))

                for i, k in enumerate(referring_fields):
                    v = _datum._tableau_fields.get(k)
                    if v is not None and isinstance(v, many_to_one):
                        if v.schema is not None and v.schema != datum._tableau_schema:
                            raise ValueError("field %s of datum %s is declared to asssociate it to %s while its container is %s" % (k, _datum, v.schema, datum._tableau_schema))
                        if referred_fields is not None and (v.other_side_fields != referred_fields[i] and v.other_side_fields[0] != name):
                            raise ValueError("field %s of datum %s is declared to asssociate it to %s via %s while expecting %s" % (k, _datum, datum._tableau_schema, v.other_side_field, referred_fields[i]))
                        m[k] = v.other_side_fields[i]
                    else:
                        if referred_fields is None:
                            m[k] = datum._tableau_id_fields[i]
                        else:
                            m[k] = referred_fields[i]
            else:
                rel = None
                for k, v in _datum._tableau_fields.items():
                    if isinstance(v, many_to_one) and v.schema == datum._tableau_schema:
                        if rel is not None:
                            raise ValueError("datum %s has more than one many-to-one associations to %s" % (_datum, datum._tableau_schema))
                        rel = v
                if rel is None:
                    raise ValueError("cannot determine the foreign key fields; datum %s has no explicit associations to %s." % (_datum, datum._tableau_schema))
                other_side_fields = rel.other_side_fields or datum._tableau_id_fields
                m = dict(zip(rel.this_side_fields, other_side_fields))

            for k1, k2 in m.items():
                setattr(_datum, k1, getattr(datum, k2))
            self(_datum)

    def _handle_many_to_many(self, datum, name, value):
        for _datum in iter(value()):
            self.suite.add_dependency(_datum._tableau_schema, datum._tableau_schema)
            self(_datum)
            these_field_values = tuple(getattr(datum, field) for field in datum._tableau_id_fields)
            those_field_values = tuple(getattr(_datum, field) for field in _datum._tableau_id_fields)
            if len(these_field_values) != len(value.this_side_fields):
                raise ValueError("%s.%s: number of referencing fields must be identical to the referenced datum's id fields" % (datum._tableau_schema, name))
            if len(those_field_values) != len(value.other_side_fields):
                raise ValueError("%s.%s: number of other side's fields must be identical to the other side's datum's id fields" % (datum._tableau_schema, name))
            if value.via is not None:
                intermediate_datum = Datum(
                    value.via,
                    value.this_side_fields + value.other_side_fields,
                    **dict(
                        chain(
                            zip(value.this_side_fields, these_field_values),
                            zip(value.other_side_fields, those_field_values)
                            )
                        )
                    )
                self(intermediate_datum)

    def _handle_many_to_one(self, datum, name, value):
        if value.schema:
            self.suite.add_dependency(datum._tableau_schema, value.schema)
        _datum = value()
        if _datum is not None:
            self(_datum)
        if value.this_side_fields is not None:
            if not value.rendered:
                if _datum is not None:
                    other_side_fields = value.other_side_fields or _datum._tableau_id_fields
                    if not other_side_fields:
                        raise ValueError("%s.%s: cannot determine other_side_fields" % (datum._tableau_schema, name))
                    if len(value.this_side_fields) != len(other_side_fields):
                        raise ValueError("%s.%s: number of this_side fields doesn't match to that of other_side field (%d != %d)" % (datum._tableau_schema, name, len(self.this_side_fields), len(other_side_fields)))
                    for k1, k2 in zip(value.this_side_fields, other_side_fields):
                        setattr(datum, k1, getattr(_datum, k2))
                else:
                    for k1 in value.this_side_fields:
                        setattr(datum, k1, None)

    def _handle(self, datum, name, value):
        if isinstance(value, one_to_many):
            self._handle_one_to_many(datum, name, value)
        elif isinstance(value, many_to_many):
            self._handle_many_to_many(datum, name, value)
        elif isinstance(value, many_to_one):
            self._handle_many_to_one(datum, name, value)
        elif isinstance(value, DynamicField):
            self._handle(datum, name, value())

    def __call__(self, datum):
        dataset = self.suite[datum._tableau_schema]
        if dataset.add(datum):
            for k, v in list(datum._tableau_fields.items()):
                self._handle(datum, k, v)
        return datum
