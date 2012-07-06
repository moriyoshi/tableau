from tableau.dataset import DataSuite, DataWalker
from tableau.containers import Datum
from tableau.declarations import one_to_many, many_to_one
from unittest import TestCase

class DataSetTest(TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

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
            self.assertEqual(i, a._fields['parent'].render())

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
        self.assertEqual(None, a._fields['parent'].render())
