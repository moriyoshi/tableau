from tableau.dataset import DataSuite, DataWalker
from tableau.containers import Datum
from tableau.declarations import one_to_many, many_to_one, auto
from tableau.sqla import newSADatum
from unittest import TestCase
from sqlalchemy.schema import MetaData, Table, Column, ForeignKey
from sqlalchemy.types import Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker, relationship

class DataSetTest(TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testAuto(self):
        a = Datum(
            'Schema',
            auto('id'),
            id=0
            )
        suite = DataSuite()
        DataWalker(suite)(a)
        self.assertEqual(1, a.id)

    def testOneToMany1(self):
        a = Datum(
            'Schema',
            'id',
            id=1,
            items=one_to_many(
                [
                    Datum(
                        'OtherSchema',
                        'id',
                        id=i,
                        value=i,
                        items=one_to_many([
                            Datum(
                                'YetOtherSchema',
                                'id',
                                id=j,
                                value=j
                                )
                            for j in range(0, 10)
                            ],
                            'other_schema_id'
                            )
                        )
                    for i in range(0, 10)
                    ],
                'schema_id'
                )
            )
        self.assertEqual(10, len(a.items))
        suite = DataSuite()
        DataWalker(suite)(a)
        for i, item in enumerate(a.items):
            self.assertEqual(i, item.id)
            self.assertEqual(1, item.schema_id)
            self.assertTrue(item in suite['OtherSchema'].data)
            for j, another_item in enumerate(item.items):
                self.assertEqual(j, another_item.id)
                self.assertEqual(item.id, another_item.other_schema_id)
                self.assertTrue(another_item in suite['YetOtherSchema'].data)
        datasets = list(suite)
        self.assertEqual(3, len(datasets))
        self.assertEqual('Schema', datasets[0].schema)
        self.assertEqual('OtherSchema', datasets[1].schema)
        self.assertEqual('YetOtherSchema', datasets[2].schema)

    def testOneToMany2(self):
        a = Datum(
            'Schema',
            'id',
            id=1,
            items=one_to_many(
                [
                    Datum(
                        'OtherSchema',
                        'id',
                        id=i,
                        value=i,
                        schema_id=many_to_one('Schema')
                        )
                    for i in range(0, 10)
                    ]
                )
            )
        self.assertEqual(10, len(a.items))
        suite = DataSuite()
        DataWalker(suite)(a)
        for i, item in enumerate(a.items):
            self.assertEqual(i, item.id)
            self.assertEqual(1, item.schema_id)
            self.assertTrue(item in suite['OtherSchema'].data)
        datasets = list(suite)
        self.assertEqual(2, len(datasets))
        self.assertEqual('Schema', datasets[0].schema)
        self.assertEqual('OtherSchema', datasets[1].schema)

    def testManyToOne1(self):
        for i in range(0, 10):
            a = Datum(
                'Schema',
                'id',
                id=1,
                parent=many_to_one(
                    Datum(
                        'OtherSchema',
                        'other_schema_primary_key',
                        other_schema_primary_key=i
                        )
                    )
                )
            suite = DataSuite()
            DataWalker(suite)(a)
            self.assertEqual(i, a._tableau_fields['parent'].render())

        for i in range(0, 10):
            a = Datum(
                'Schema',
                'id',
                id=1,
                parent=many_to_one(
                    Datum(
                        'OtherSchema',
                        'other_schema_primary_key',
                        other_schema_primary_key=i
                        ),
                    'parent_id',
                    )
                )
            suite = DataSuite()
            DataWalker(suite)(a)
            self.assertEqual('OtherSchema', a.parent._tableau_schema)
            self.assertEqual(i, a.parent_id)

    def testManyToOne2(self):
        a = Datum(
            'Schema',
            'id',
            id=1,
            parent=many_to_one(
                None
                )
            )

        suite = DataSuite()
        DataWalker(suite)(a)
        self.assertEqual(None, a._tableau_fields['parent'].render())

    def testManyToOne3(self):
        metadata = MetaData()
        Table('Schema', metadata,
            Column('id', Integer, primary_key=True),
            Column('parent_id', Integer)
            )
        SADatum = newSADatum(metadata)
        a = SADatum(
            'Schema',
            'id',
            id=1,
            parent=many_to_one(
                Datum('Foo', 'id', id=1),
                'parent_id'
                )
            )
        suite = DataSuite()
        DataWalker(suite)(a)
        self.assertEqual(1, a._tableau_fields['parent'].render())

class SADatumTest(TestCase):
    def setUp(self):
        self.metadata = MetaData()
        self.declarative_base = declarative_base(metadata=self.metadata)

    def testPrimaryKey1(self):
        table = Table('Test', self.metadata,
            Column('id', Integer, primary_key=True)
            )
        SADatum = newSADatum(self.metadata)
        try:
            SADatum(
                'Test',
                'oops'
                )
            self.fail("No expection raised")
        except ValueError, e:
            self.assertEqual("id_fields does not match to the table definition ([oops] != [id])", e.args[0])

    def testPrimaryKey2(self):
        table = Table('Test', self.metadata,
            Column('id1', Integer, primary_key=True),
            Column('id2', Integer, primary_key=True)
            )
        SADatum = newSADatum(self.metadata)
        try:
            SADatum(
                'Test',
                'id1'
                )
            self.fail("No expection raised")
        except ValueError, e:
            self.assertEqual("id_fields does not match to the table definition ([id1] != [id1,id2])", e.args[0])

    def testPrimaryKey3(self):
        table = Table('Test', self.metadata,
            Column('id', Integer, primary_key=True)
            )
        SADatum = newSADatum(self.metadata)
        try:
            SADatum(
                'Test',
                ('id1', 'id2'),
                )
            self.fail("No expection raised")
        except ValueError, e:
            self.assertEqual("id_fields does not match to the table definition ([id1,id2] != [id])", e.args[0])

    def testPrimaryKey4(self):
        table = Table('Test', self.metadata,
            Column('id', Integer, primary_key=True)
            )
        SADatum = newSADatum(self.metadata)
        datum = SADatum('Test')
        self.assertEqual(('id', ), datum._tableau_id_fields)

    def testWithSchemaOnly1(self):
        SADatum = newSADatum(self.metadata)
        try:
            SADatum(
                'Test',
                'id',
                field='test'
                )
            self.fail("No expection raised")
        except ValueError, e:
            self.assertEqual("Test is not defined in the metadata", e.args[0])

    def testWithSchemaOnly2(self):
        table = Table('Test', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('field', String)
            )
        SADatum = newSADatum(self.metadata)
        datum = SADatum(
            'Test',
            'id',
            field='test'
            )
        self.assertEqual('Test', datum._tableau_schema)
        self.assertEqual(table, datum._tableau_table)

    def testWithSchemaOnly3(self):
        table = Table('Test', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('field', String)
            )
        SADatum = newSADatum(self.metadata)
        datum = SADatum(
            table,
            'id',
            field='test'
            )
        self.assertEqual('Test', datum._tableau_schema)
        self.assertEqual(table, datum._tableau_table)

    def testWithDeclarative1(self):
        class Test(self.declarative_base):
            __tablename__ = 'Oops'
            id = Column(Integer, primary_key=True)
        SADatum = newSADatum(self.metadata, self.declarative_base)
        try:
            SADatum(
                'Test',
                'id',
                field='test'
                )
            self.fail("No expection raised")
        except ValueError, e:
            self.assertEqual("Test is not defined in the metadata", e.args[0])

    def testWithDeclarative2(self):
        table = Table('Test', self.metadata, Column('id', Integer, primary_key=True))
        class Test(self.declarative_base):
            __tablename__ = 'Oops'
            id = Column(Integer, primary_key=True)
        SADatum = newSADatum(self.metadata, self.declarative_base)
        try:
            SADatum(
                'Test',
                'id',
                field='test'
                )
            self.fail("No expection raised")
        except ValueError, e:
            self.assertEqual("declarative class for Test is not in the class registry", e.args[0])

    def testWithDeclarative3(self):
        class Test(self.declarative_base):
            __tablename__ = 'Test'
            id = Column(Integer, primary_key=True)
            field = Column(String)
        SADatum = newSADatum(self.metadata, self.declarative_base)
        datum = SADatum(
            'Test',
            'id',
            field='test'
            )
        self.assertEqual('Test', datum._tableau_schema)
        self.assertEqual(Test.__table__, datum._tableau_table)
        self.assertEqual('test', datum.field)
        self.assertIsInstance(datum, Test)

    def testIfDeclarativeIsWalkable(self):
        class Test(self.declarative_base):
            __tablename__ = 'Test'
            id = Column(Integer, primary_key=True)
            field = Column(String)
        SADatum = newSADatum(self.metadata, self.declarative_base)
        datum = SADatum(
            'Test',
            auto('id'),
            field='test'
            )
        self.assertEqual('Test', datum._tableau_schema)
        self.assertEqual(Test.__table__, datum._tableau_table)
        self.assertEqual('test', datum.field)
        self.assertIsInstance(datum, Test)
        suite = DataSuite()
        DataWalker(suite)(datum)
        self.assertEqual(1, datum.id)

    def testIfDeclarativeIsAddableToSession(self):
        class Foo(self.declarative_base):
            __tablename__ = 'Foo'
            id = Column(Integer, primary_key=True)
            field = Column(String)
            bars = relationship('Bar')
            def some_model_specific_method(self):
              return self.field

        class Bar(self.declarative_base):
            __tablename__ = 'Bar'
            id = Column(Integer, primary_key=True)
            type = Column(String)
            foo_id = Column(Integer, ForeignKey('Foo.id'))
            __mapper_args__ = { 'polymorphic_on': type }

        class Foobar(Bar):
            __tablename__ = 'Foobar'
            __mapper_args__ = {'polymorphic_identity': 'foobar'}
            id = Column(Integer, ForeignKey('Bar.id'), primary_key=True)

        SADatum = newSADatum(self.metadata, self.declarative_base)
        datum = SADatum(
            'Foo',
            auto('id'),
            field='test',
            bars=one_to_many([
                SADatum(
                    'Bar',
                    auto('id')
                    ),
                SADatum(
                    'Foobar',
                    auto('id')
                    )
                ],
                'foo_id'
                )
            )
        self.assertEqual('Foo', datum._tableau_schema)
        self.assertEqual(Foo.__table__, datum._tableau_table)
        self.assertEqual('test', datum.field)
        self.assertIsInstance(datum, Foo)
        self.assertEqual('test', datum.some_model_specific_method())
        self.assertIsInstance(datum.bars[0], Bar)
        self.assertIsInstance(datum.bars[1], Bar)
        engine = create_engine('sqlite+pysqlite:///', echo=True)
        session = sessionmaker(engine)()
        self.metadata.create_all(engine)
        session.add(datum)
        session.flush()
        self.assertEqual(1, datum.id)
        self.assertEqual(1, datum.bars[0].id)
        self.assertEqual(2, datum.bars[1].id)

