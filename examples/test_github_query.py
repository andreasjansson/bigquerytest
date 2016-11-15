from bigquerytest import BigQueryTestCase

class TestGithubQuery(BigQueryTestCase):

    project = 'CHANGEME'
    dataset = 'CHANGEME'

    def test_github_query(self):

        sql = '''
            SELECT
              repository.name AS name,
              array_length(payload.pages) AS count
            FROM
              `bigquery-public-data.samples.github_nested`
            ORDER BY
              2 DESC
            LIMIT
              3
        '''

        self.mock_table(
            'bigquery-public-data.samples.github_nested',
            '''
            repository.name  payload.pages.title
            foo              foo1
                             foo2
            bar              bar1
            baz
            qux              qux1
                             qux2
                             qux3
            ''', cleanup=False)

        expected = '''
            name  count
            qux   3
            foo   2
            bar   1
        '''

        actual = self.query(sql)

        self.assert_tables_equal(actual, expected)
