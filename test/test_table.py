import unittest
from bigquerytest.table import (
    parse_column_header,
    parse_row,
    BigQueryTestSchemaField,
    update_record,
    table_from_definition_string,
    table_from_api_response,
    flatten_table,
    table_prettyprint,
    narrow_fields_to_columns
)

class TestBigQueryTestTable(unittest.TestCase):

    def test_parse_column_header(self):
        row = ' c1    col2  Column-4 5'
        columns, offsets = parse_column_header(row)
        self.assertEquals(columns, ['c1', 'col2', 'Column-4', '5'])
        self.assertEquals(offsets, [1, 7, 13, 22])

    def test_parse_good_row(self):
        columns = ['c1', 'c2.a', 'c2.b']
        offsets = [2, 6, 11]
        row = '  abc de   fghi'
        self.assertEquals(parse_row(row, columns, offsets), {'c1': 'abc', 'c2.a': 'de', 'c2.b': 'fghi'})

    def test_parse_good_row_with_space(self):
        columns = ['c1', 'c2.a', 'c2.b']
        offsets = [2, 6, 11]
        row = '  abc      fghi'
        self.assertEquals(parse_row(row, columns, offsets), {'c1': 'abc', 'c2.b': 'fghi'})

    def test_parse_bad_row_end(self):
        columns = ['c1', 'c2.a', 'c2.b']
        offsets = [2, 6, 11]
        row = '  ab cde   fghi'
        with self.assertRaises(ValueError):
            parse_row(row, columns, offsets)

    def test_parse_bad_row_beginning(self):
        columns = ['c1', 'c2.a', 'c2.b']
        offsets = [2, 6, 11]
        row = '  abc  de  fghi'
        with self.assertRaises(ValueError):
            parse_row(row, columns, offsets)

    def test_update_record_flat_empty(self):
        schema = [
            BigQueryTestSchemaField('c1', 'string', 'c1', None, False, False, False),
            BigQueryTestSchemaField('c2', 'integer', 'c2', None, False, False, False),
        ]
        record = {}
        row = {'c1': 'foo', 'c2': 123}
        update_record(schema, row, record)
        self.assertEquals(record, {'c1': 'foo', 'c2': 123})

    def test_update_record_repeated(self):
        schema = [
            BigQueryTestSchemaField('c1', 'string', 'c1', None, False, False, False),
            BigQueryTestSchemaField('c2', 'integer', 'c2', None, False, True, True),
        ]
        record = {}
        row = {'c1': 'foo', 'c2': 123}
        update_record(schema, row, record)
        self.assertEquals(record, {'c1': 'foo', 'c2': [123]})

    def test_update_record_repeated_multiple(self):
        schema = [
            BigQueryTestSchemaField('c1', 'string', 'c1', None, False, False, False),
            BigQueryTestSchemaField('c2', 'integer', 'c2', None, False, True, True),
        ]
        record = {'c1': 'foo', 'c2': [123]}
        row = {'c2': 456}
        update_record(schema, row, record)
        self.assertEquals(record, {'c1': 'foo', 'c2': [123, 456]})

    def test_update_record_recursive(self):
        schema = [
            BigQueryTestSchemaField('c1', 'string', 'c1', None, False, False, False),
            BigQueryTestSchemaField('c2', 'record', 'c2', [
                BigQueryTestSchemaField('x', 'integer', 'c2.x', None, False, False, False),
                BigQueryTestSchemaField('y', 'integer', 'c2.y', None, False, False, False),
            ], False, False, False),
        ]
        record = {}
        row = {'c1': 'foo', 'c2.x': 123, 'c2.y': 456}
        update_record(schema, row, record)
        self.assertEquals(record, {'c1': 'foo', 'c2': {'x': 123, 'y': 456}})

    def test_update_record_recursive_repeated(self):
        schema = [
            BigQueryTestSchemaField('c1', 'string', 'c1', None, False, False, False),
            BigQueryTestSchemaField('c2', 'record', 'c2', [
                BigQueryTestSchemaField('x', 'integer', 'c2.x', None, False, False, False),
                BigQueryTestSchemaField('y', 'integer', 'c2.y', None, False, True, True),
            ], False, False, False),
        ]
        record = {}
        row = {'c1': 'foo', 'c2.x': 123, 'c2.y': 456}
        update_record(schema, row, record)
        self.assertEquals(record, {'c1': 'foo', 'c2': {'x': 123, 'y': [456]}})

    def test_update_record_recursive_repeated_multiple(self):
        schema = [
            BigQueryTestSchemaField('c1', 'string', 'c1', None, False, False, False),
            BigQueryTestSchemaField('c2', 'record', 'c2', [
                BigQueryTestSchemaField('x', 'integer', 'c2.x', None, False, False, False),
                BigQueryTestSchemaField('y', 'integer', 'c2.y', None, False, True, True),
            ], False, False, False),
        ]
        record = {'c1': 'foo', 'c2': {'x': 123, 'y': [456]}}
        row = {'c2.y': 789}
        update_record(schema, row, record)
        self.assertEquals(record, {'c1': 'foo', 'c2': {'x': 123, 'y': [456, 789]}})

    def test_update_record_deep_nested_nullable(self):
        schema = [
            BigQueryTestSchemaField('repository', 'record', 'repository', [
                BigQueryTestSchemaField('name', 'string', 'repository.name', None, True, False, False),
            ], True, False, True),
            BigQueryTestSchemaField('payload', 'record', 'payload', [
                BigQueryTestSchemaField('pages', 'record', 'payload.pages', [
                    BigQueryTestSchemaField('title', 'string', 'payload.pages.title', None, True, False, True),
                ], False, True, True),
            ], True, False, False),
        ]
        record = {'repository': {'name': 'foo'}, 'payload': {'pages': [{'title': 'foo1'}]}}
        row = {'payload.pages.title': 'foo2'}
        update_record(schema, row, record)
        self.assertEquals(record, {'repository': {'name': 'foo'}, 'payload': {'pages': [{'title': 'foo1'}, {'title': 'foo2'}]}})

    def test_update_record_recursive_deep_repeated_new(self):
        schema = [
            BigQueryTestSchemaField('c1', 'string', 'c1', None, False, False, False),
            BigQueryTestSchemaField('c2', 'record', 'c2', [
                BigQueryTestSchemaField('x', 'record', 'c2.x', [
                    BigQueryTestSchemaField('xx', 'integer', 'c2.x.xx', None, False, False, True),
                ], False, True, True),
                BigQueryTestSchemaField('y', 'string', 'c2.y', None, False, False, True),
            ], False, True, True),
        ]
        record = {'c1': 'foo', 'c2': [{'x': [{'xx': 1}], 'y': 'bar'}]}
        row = {'c2.x.xx': 2, 'c2.y': 'baz'}
        update_record(schema, row, record)
        self.assertEquals(record, {'c1': 'foo', 'c2': [{'x': [{'xx': 1}], 'y': 'bar'}, {'x': [{'xx': 2}], 'y': 'baz'}]})

    def test_table_from_definition_string(self):
        schema = [
            BigQueryTestSchemaField('c1', 'string', 'c1', None, False, False, False),
            BigQueryTestSchemaField('c2', 'record', 'c2', [
                BigQueryTestSchemaField('x', 'record', 'c2.x', [
                    BigQueryTestSchemaField('xx', 'integer', 'c2.x.xx', None, False, False, True),
                ], False, True, True),
                BigQueryTestSchemaField('y', 'string', 'c2.y', None, False, False, True),
            ], False, True, True),
        ]

        table_definition = '''
        c1   c2.x.xx  c2.y
        foo  1        a
        bar           b
        baz  3        b
             4
             5        d
        '''

        expected = [
            {'c1': 'foo', 'c2': [{'x': [{'xx': 1}], 'y': 'a'}]},
            {'c1': 'bar', 'c2': [{'y': 'b'}]},
            {'c1': 'baz', 'c2': [{'x': [{'xx': 3}], 'y': 'b'},
                                 {'x': [{'xx': 4}]},
                                 {'x': [{'xx': 5}], 'y': 'd'}]},
        ]

        self.assertEquals(table_from_definition_string(table_definition, schema).data, expected)

    def test_table_from_definition_string_deep_nested(self):
        schema = [
            BigQueryTestSchemaField('repository', 'record', 'repository', [
                BigQueryTestSchemaField('name', 'string', 'repository.name', None, True, False, False),
            ], True, False, False),
            BigQueryTestSchemaField('payload', 'record', 'payload', [
                BigQueryTestSchemaField('pages', 'record', 'payload.pages', [
                    BigQueryTestSchemaField('title', 'string', 'payload.pages.title', None, True, False, True),
                ], False, True, True),
            ], True, False, False),
        ]

        table_definition = '''
            repository.name  payload.pages.title
            foo              foo1
                             foo2
            bar              bar1
            baz
            qux              qux1
                             qux2
                             qux3
        '''

        expected = [
            {'payload': {'pages': [{'title': 'foo1'}, {'title': 'foo2'}]}, 'repository': {'name': 'foo'}},
            {'payload': {'pages': [{'title': 'bar1'}]}, 'repository': {'name': 'bar'}},
            {'repository': {'name': 'baz'}},
            {'payload': {'pages': [{'title': 'qux1'}, {'title': 'qux2'}, {'title': 'qux3'}]}, 'repository': {'name': 'qux'}}
        ]

        self.assertEquals(table_from_definition_string(table_definition, schema).data, expected)

    def test_table_from_api_response(self):
        schema = [
            BigQueryTestSchemaField('c1', 'string', 'c1', None, False, False, False),
            BigQueryTestSchemaField('c2', 'record', 'c2', [
                BigQueryTestSchemaField('x', 'record', 'c2.x', [
                    BigQueryTestSchemaField('xx', 'integer', 'c2.x.xx', None, False, False, True),
                ], False, True, True),
                BigQueryTestSchemaField('y', 'string', 'c2.y', None, False, True, True),
            ], False, True, True),
        ]

        response = [
            {'f': [{'v': 'foo'},
                   {'v': [{'v': {'f': [{'v': [{'v': {'f': [{'v': '1'}]}}]},
                                       {'v': 'a'}]}}]}]},
            {'f': [{'v': 'baz'},
                   {'v': [{'v': {'f': [{'v': [{'v': {'f': [{'v': '3'}]}},
                                              {'v': {'f': [{'v': '4'}]}}]},
                                       {'v': 'b'}]}},
                          {'v': {'f': [{'v': [{'v': {'f': [{'v': '5'}]}}]},
                                       {'v': 'd'}]}}]}]},
            {'f': [{'v': 'bar'},
                   {'v': [{'v': {'f': [{'v': []}, {'v': 'b'}]}}]}]}
        ]

        expected = [
            {'c1': 'foo', 'c2': [{'x': [{'xx': 1}], 'y': 'a'}]},
            {'c1': 'baz', 'c2': [{'x': [{'xx': 3},
                                        {'xx': 4}], 'y': 'b'},
                                 {'x': [{'xx': 5}], 'y': 'd'}]},
            {'c1': 'bar', 'c2': [{'y': 'b'}]},
        ]

        self.assertEquals(table_from_api_response(response, schema).data, expected)

    def test_prettyprint_flat(self):
        schema = [
            BigQueryTestSchemaField('c1', 'string', 'c1', None, False, False, False),
            BigQueryTestSchemaField('c2', 'integer', 'c2', None, False, False, False),
        ]

        table = [
            {'c1': 'foo', 'c2': 123},
            {'c1': 'bar', 'c2': 456},
        ]

        expected = '''
c1   c2
foo  123
bar  456
'''

        flat = flatten_table(table, schema)
        self.assertEquals(table_prettyprint(flat), expected.strip())

    def test_prettyprint_records_flat(self):
        schema = [
            BigQueryTestSchemaField('c1', 'record', 'c1', [
                BigQueryTestSchemaField('a', 'string', 'c1.a', None, False, False, False),
                BigQueryTestSchemaField('b', 'string', 'c1.b', None, False, False, False),
            ], False, False, False),
            BigQueryTestSchemaField('c2', 'integer', 'c2', None, False, False, False),
        ]

        table = [
            {'c1': {'a': 'foo', 'b': 'bar'}, 'c2': 123},
            {'c1': {'a': 'baz', 'b': 'qux'}, 'c2': 456},
        ]

        expected = '''
c1.a  c1.b  c2
foo   bar   123
baz   qux   456
'''

        flat = flatten_table(table, schema)
        self.assertMultiLineEqual(table_prettyprint(flat), expected.strip())

    def test_prettyprint_repeated(self):
        schema = [
            BigQueryTestSchemaField('c1', 'string', 'c1', None, False, False, False),
            BigQueryTestSchemaField('c2', 'integer', 'c2', None, False, True, True),
        ]

        table = [
            {'c1': 'a', 'c2': [1, 2, 3]},
            {'c1': 'b', 'c2': [4, 5]},
        ]

        expected = '''
c1  c2
a   1
    2
    3
b   4
    5
'''

        flat = flatten_table(table, schema)
        self.assertMultiLineEqual(table_prettyprint(flat), expected.strip())

    def test_prettyprint_repeated_single(self):
        schema = [
            BigQueryTestSchemaField('c1', 'string', 'c1', None, False, False, False),
            BigQueryTestSchemaField('c2', 'integer', 'c2', None, False, True, True),
        ]

        table = [
            {'c1': 'a', 'c2': [1]},
            {'c1': 'b', 'c2': [2]},
            {'c1': 'c', 'c2': [3]},
        ]

        expected = '''
c1  c2
a   1
b   2
c   3
'''

        flat = flatten_table(table, schema)
        self.assertMultiLineEqual(table_prettyprint(flat), expected.strip())

    def test_prettyprint_repeated_nested(self):
        schema = [
            BigQueryTestSchemaField('c1', 'record', 'c1', [
                BigQueryTestSchemaField('a', 'integer', 'c1.a', None, False, True, True),
                BigQueryTestSchemaField('b', 'integer', 'c1.b', None, False, False, False),
            ], False, True, True),
            BigQueryTestSchemaField('c2', 'integer', 'c2', None, False, False, False),
        ]

        table = [
            {'c1': [{'a': [1, 2], 'b': 100}, {'a': [4], 'b': 200}], 'c2': 0},
            {'c1': [{'a': [3], 'b': 200}], 'c2': 5},
            {'c1': [{'a': [], 'b': 300}, {'a': [1, 2], 'b': 10}], 'c2': 10},
        ]

        expected = '''
c1.a  c1.b  c2
1     100   0
2
4     200
3     200   5
      300   10
1     10
2
'''

        flat = flatten_table(table, schema)
        self.assertMultiLineEqual(table_prettyprint(flat), expected.strip())

    def test_table_prettyprint_complex(self):
        schema = [
            BigQueryTestSchemaField('c1', 'string', 'c1', None, False, False, False),
            BigQueryTestSchemaField('c2', 'record', 'c2', [
                BigQueryTestSchemaField('x', 'record', 'c2.x', [
                    BigQueryTestSchemaField('xx', 'integer', 'c2.x.xx', None, False, False, True),
                ], False, True, True),
                BigQueryTestSchemaField('y', 'string', 'c2.y', None, False, False, True),
            ], False, True, True),
        ]

        table = [
            {'c1': 'foo', 'c2': [{'x': [{'xx': 1}], 'y': 'a'}]},
            {'c1': 'bar', 'c2': [{'y': 'b'}, {'y': 'z'}]},
            {'c1': 'baz', 'c2': [{'x': [{'xx': 3},
                                        {'xx': 4}], 'y': 'b'},
                                 {'x': [{'xx': 5}], 'y': 'd'}]},
        ]

        expected = '''
c1   c2.x.xx  c2.y
foo  1        a
bar           b
              z
baz  3        b
     4
     5        d'''

        flat = flatten_table(table, schema)
        self.assertEquals(table_prettyprint(flat), expected.lstrip())

    def test_table_prettyprint_stable(self):
        schema = [
            BigQueryTestSchemaField('c1', 'string', 'c1', None, False, False, False),
            BigQueryTestSchemaField('c2', 'record', 'c2', [
                BigQueryTestSchemaField('x', 'record', 'c2.x', [
                    BigQueryTestSchemaField('xx', 'integer', 'c2.x.xx', None, False, False, True),
                ], False, True, True),
                BigQueryTestSchemaField('y', 'string', 'c2.y', None, False, False, True),
            ], False, True, True),
        ]

        table = '''
c1   c2.x.xx  c2.y
foo  1        a
bar           b
              z
baz  3        b
     4
     5        d'''

        self.assertEquals(
            table_from_definition_string(table, schema).prettyprint(),
            table.lstrip())

    def test_narrow_fields_to_columns(self):
        schema = [
            BigQueryTestSchemaField('c1', 'string', 'c1', None, False, False, False),
            BigQueryTestSchemaField('c2', 'record', 'c2', [
                BigQueryTestSchemaField('x', 'record', 'c2.x', [
                    BigQueryTestSchemaField('x1', 'integer', 'c2.x.x1', None, False, False, True),
                    BigQueryTestSchemaField('x2', 'integer', 'c2.x.x2', None, False, False, True),
                ], False, True, True),
                BigQueryTestSchemaField('y', 'string', 'c2.y', None, False, False, True),
                BigQueryTestSchemaField('z', 'string', 'c2.z', None, False, False, True),
            ], False, True, True),
            BigQueryTestSchemaField('c3', 'record', 'c3', [
                BigQueryTestSchemaField('x', 'integer', 'c3.x', None, False, False, False),
            ], False, False, False),
        ]

        columns = ['c1', 'c2.x.x2', 'c2.z']

        expected = [
            BigQueryTestSchemaField('c1', 'string', 'c1', None, False, False, False),
            BigQueryTestSchemaField('c2', 'record', 'c2', [
                BigQueryTestSchemaField('x', 'record', 'c2.x', [
                    BigQueryTestSchemaField('x2', 'integer', 'c2.x.x2', None, False, False, True),
                ], False, True, True),
                BigQueryTestSchemaField('z', 'string', 'c2.z', None, False, False, True),
            ], False, True, True),
        ]

        self.assertEquals(narrow_fields_to_columns(schema, columns), expected)
