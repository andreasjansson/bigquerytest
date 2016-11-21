# bigquerytest

[![Build Status](https://travis-ci.org/andreasjansson/bigquerytest.svg?branch=master)](https://travis-ci.org/andreasjansson/bigquerytest)

[![PyPI version](https://badge.fury.io/py/bigquerytest.svg)](https://badge.fury.io/py/bigquerytest)

Test complicated BigQuery SQL by mocking tables with deterministic, readable data.

## Usage by example

```python
from bigquerytest import BigQueryTestCase

# BigQueryTestCase extends unittest.TestCase
class TestGithubQuery(BigQueryTestCase):

    # bigquerytest creates actual BQ tables. You need to tell it which project
    # and dataset to put the temporary tables in.
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

        # Replace "bigquery-public-data.samples.github_nested" with mock data.
        self.mock_table(

            # The table name to mock
            'bigquery-public-data.samples.github_nested',

            # Mock data is created in this human-readable format. The left
            # hand side must align. Empty space is used for repeated fields.
            '''
            repository.name  payload.pages.title
            foo              foo1
                             foo2
            bar              bar1
            baz
            qux              qux1
                             qux2
                             qux3
            ''',

            # By default tables are deleted after the test. You can save
            # time by setting cleanup=False, which will leave the table
            # around after the test.
            cleanup=False)

        # Another table in human-readable format.
        expected = '''
        # You can put comments in table definitions with "#".
        # Empty lines are ignored

            name  count
            qux   3
            foo   2
            bar   1
        '''

        # Run the query with mocked table references.
        actual = self.query(sql)

        # Check that the returned table matches the expectation.
        self.assert_tables_equal(actual, expected)
```

## Installation

```
pip install bigquerytest
```

## Requirements

Python 2.7 or above.
