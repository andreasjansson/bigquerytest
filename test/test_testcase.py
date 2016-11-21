import unittest
from bigquerytest.testcase import BigQueryTestCase
from mock import patch


class BigQueryTestCaseDummy(BigQueryTestCase):
    project = 'my-project'
    dataset = 'my_dataset'
    def __init__(self):
        super(BigQueryTestCaseDummy, self).__init__(methodName='__class__')


class BigQueryTestCaseLegacyDummy(BigQueryTestCaseDummy):
    use_legacy_sql = True


class TestBigQueryTestCase(unittest.TestCase):

    @patch('google.cloud.bigquery.Client')
    def test_replace_tables_in_query(self, mock_bigquery_client):
        test = BigQueryTestCaseDummy()
        test._mock_tables = {
            '`abc.def.ghi`': 'mock1',
            '[jkl:mno.pqr]': 'mock2',
            '`suv.wxy`': 'mock3',
            'my-project.abc.xxx': 'mock4',
        }

        sql = '''
            select * from
                abc.def.ghi,
                `jkl.mno.pqr`,
                [my-project:suv.wxy],
                abc.xxx
        '''

        expected = '''
            select * from
                `my-project.my_dataset.mock1`,
                `my-project.my_dataset.mock2`,
                `my-project.my_dataset.mock3`,
                `my-project.my_dataset.mock4`
        '''

        self.assertMultiLineEqual(test._replace_tables_in_query(sql), expected)

    @patch('google.cloud.bigquery.Client')
    def test_replace_tables_in_query_legacy(self, mock_bigquery_client):
        test = BigQueryTestCaseLegacyDummy()
        test._mock_tables = {
            '`abc.def`': 'mock1',
        }

        sql = '''
            select * from
                abc.def
        '''

        expected = '''
            select * from
                [my-project:my_dataset.mock1]
        '''

        self.assertMultiLineEqual(test._replace_tables_in_query(sql), expected)
