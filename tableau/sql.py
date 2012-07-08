# encoding: utf-8

import logging
import datetime
from tableau.declarations import DynamicField

class InsertStmtBuilder(object):
    def __init__(self, builder):
        self.builder = builder
        self.prev_table = None
        self.prev_keys = u''
        self.nbytes_sent = 0

    def flush(self):
        if self.prev_table is not None:
            self.write(";\n");
        self.prev_table = None
        self.prev_keys = u''
        self.nbytes_sent = 0

    def write(self, str):
        self.builder.out.write(str)
        self.nbytes_sent += len(str)

    def __call__(self, table, _values):
        encoding = self.builder.encoding
        values = []
        keys = u''
        value_len = 0
        for k, v in _values:
            v_ = self.builder.put_scalar(v)
            keys += k
            values.append((k, v_))
            value_len += len(v_)

        if self.nbytes_sent >= 131072:
            self.flush() 

        if self.prev_table != table or self.prev_keys != keys:
            self.flush()
            self.write("INSERT INTO `%s` (%s) VALUES\n" % (
                table.encode(encoding),
                ', '.join(self.builder.put_identifier(k) for k, v in values)))
        else:
            self.write(",\n")

        if value_len < 1024:
            self.write("(" + ", ".join(v for _, v in values) + ")")
        else:
            self.write("(")
            first = True
            for _, v in values:
                if not first:
                    self.write(", ")
                self.write(v)
                first = False
            self.write(")")
        self.prev_table = table
        self.prev_keys = keys

class SQLBuilder(object):
    def __init__(self, out, encoding=None):
        self.out = out
        self.encoding = encoding or self.out.encoding
        self.last_stmt = None

    def __del__(self):
        self.flush()

    def put_identifier(self, name):
        if isinstance(name, str):
            return "`%s`" % name
        elif isinstance(name, unicode):
            return "`%s`" % name.encode(self.encoding)
        else:
            raise TypeError("Unsupported type: " + type(name).__name__)

    def put_scalar(self, scalar):
        if isinstance(scalar, str):
            return "'%s'" % scalar.replace("'", "''")
        elif isinstance(scalar, unicode):
            return "'%s'" % scalar.replace(u"'", u"''").encode(self.encoding)
        elif isinstance(scalar, datetime.datetime):
            return "'%s'" % scalar.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(scalar, datetime.date):
            return "'%s'" % scalar.strftime("%Y-%m-%d")
        elif isinstance(scalar, datetime.time):
            return "'%s'" % scalar.strftime("%H:%M:%S")
        elif isinstance(scalar, (float, int, long)):
            return '%d' % scalar
        elif scalar is None:
            return 'NULL'
        else:
            raise TypeError("Unsupported type: " + type(scalar).__name__)

    def insert(self, table, values):
        if not isinstance(self.last_stmt, InsertStmtBuilder):
            if self.last_stmt is not None:
                self.last_stmt.flush()
            self.last_stmt = InsertStmtBuilder(self)
        self.last_stmt(table, values)

    def flush(self):
        if self.last_stmt is not None:
            self.last_stmt.flush()
        self.last_stmt = None
         
class SQLGenerator(object):
    logger = logging.getLogger('tableau.SQLGenerator')

    def __init__(self, out, builder_impl=SQLBuilder, **kwargs):
        self.out = out
        self.builder_impl = builder_impl
        self.kwargs = kwargs

    def __call__(self, suite):
        builder = self.builder_impl(self.out, **self.kwargs)
        for dataset in suite:
            for data in dataset:
                values = [] 
                for k, v in sorted(data._tableau_fields.iteritems(),
                                   lambda a, b: \
                                     -1 if a[0] in data._tableau_id_fields \
                                       else (1 if b[0] in data._tableau_id_fields \
                                                else cmp(a[0], b[0]))):
                    if isinstance(v, DynamicField):
                        if not v.rendered:
                            continue
                        v = v.render()
                    values.append((k, v))
                builder.insert(data._tableau_schema, values)
        builder.flush()
