import logging
import json
from StringIO import StringIO
import re
import time
import unittest
from google.cloud import bigquery

from .table import (
    table_from_definition_string,
    table_from_executed_query,
    schema_from_bigquery_schema,
    BigQueryTestTable,
    get_column_widths
)


class BigQueryTestCase(unittest.TestCase):

    TABLE_REGEX = re.compile(r'''
        [\[`]?(?:(?P<dataset>[-a-zA-Z0-9_]+)[:.])?
              (?P<project>[a-zA-Z0-9_]+)\.
              (?P<table>[a-zA-Z0-9_]+)[\]`]?
    ''', re.VERBOSE)

    @property
    def project(self):
        raise NotImplementedError()

    @property
    def dataset(self):
        raise NotImplementedError()

    @property
    def table_prefix(self):
        return 'bigquery_test_mock'

    @property
    def use_legacy_sql(self):
        return False

    def __init__(self, *args, **kwargs):
        super(BigQueryTestCase, self).__init__(*args, **kwargs)
        self.addTypeEqualityFunc(BigQueryTestTable, 'assert_tables_equal')
        self._bigquery_client = bigquery.Client(project=self.project)
        self._log = logging.getLogger('bigquerytest')

    def setUp(self):
        self._mock_tables = {}

    def mock_table(self, table_id, table_definition, cleanup=True):
        schema = self._load_schema(table_id)
        table = table_from_definition_string(table_definition, schema)
        mock_table_name = self._get_table_name(table, table_id)
        self._create_table(mock_table_name, table)
        self._mock_tables[table_id] = mock_table_name

        if cleanup:
            self.addCleanup(self._delete_table, mock_table_name)

    def query(self, sql):
        sql = self._replace_tables_in_query(sql)
        self._log.debug(sql)
        query = self._bigquery_client.run_sync_query(sql)
        query.use_legacy_sql = self.use_legacy_sql
        query.run()
        query_fetch_data(self._bigquery_client, query)
        return table_from_executed_query(query)

    def assert_tables_equal(self, actual, expected):
        if isinstance(expected, basestring):
            expected = table_from_definition_string(expected, actual.schema)

        columns1 = actual.get_column_names()
        columns2 = expected.get_column_names()
        self.assertEquals(columns1, columns2)

        flat1 = actual.flatten()
        flat2 = expected.flatten()
        concat = flat1 + flat2
        column_widths = get_column_widths(concat)

        pretty1 = actual.prettyprint(column_widths)
        pretty2 = expected.prettyprint(column_widths)

        self.assertMultiLineEqual(pretty1, pretty2)

    def _create_table(self, mock_table_name, table):
        bq_schema = table.get_bigquery_schema()
        bq_table = self._table(mock_table_name, bq_schema)
        if bq_table.exists():
            self._log.info('Table already exists, not creating: %s', mock_table_name)
            return

        bq_table.create()
        self._log.debug('Creating table: %s', mock_table_name)
        while not bq_table.exists():
            bq_table.reload()
            time.sleep(5)

        f = StringIO('\n'.join([json.dumps(row) for row in table.data]))
        op = bq_table.upload_from_file(
            f, 'NEWLINE_DELIMITED_JSON', size=f.len)

        self._log.debug('Uploading data to table: %s', mock_table_name)
        while op.state != 'DONE':
            op.reload()
            time.sleep(5)

    def _delete_table(self, table_name):
        table = self._table(table_name)
        table.delete()
        self._log.debug('Deleting table: %s', table_name)
        while table.exists():
            table.reload()
            time.sleep(5)

    def _get_table_name(self, table, table_id):
        match = self.TABLE_REGEX.match(table_id)
        if not match:
            raise ValueError('Bad table name: %s' % table_id)
        _, _, table_name = match.groups()
        return '%s_%s_%s' % (self.table_prefix, table_name, table.get_hash())

    def _replace_tables_in_query(self, sql):
        replacements = {}

        for table_name, mock_table_name in self._mock_tables.items():
            mock_table_id = (
                '[%s:%s.%s]' if self.use_legacy_sql else '`%s.%s.%s`'
            ) % (self.project, self.dataset, mock_table_name)

            match = self.TABLE_REGEX.match(table_name)
            if not match:
                raise ValueError('Bad table name: %s' % table_name)
            project, dataset, table = match.groups()
            if project is None:
                project = self.project

            for match in self.TABLE_REGEX.finditer(sql):
                sql_project, sql_dataset, sql_table = match.groups()
                if sql_project is None:
                    sql_project = self.project

                start, end = match.span()

                if (sql_project == project and sql_dataset == dataset and
                    sql_table == table):
                    replacements[sql[start:end]] = mock_table_id
                else:
                    self._log.info('Not mocking table: %s' % sql[start:end])

        for string, replacement in replacements.items():
            sql = sql.replace(string, replacement)

        return sql

    def _load_schema(self, table_id):
        match = self.TABLE_REGEX.match(table_id)
        if not match:
            raise ValueError('Bad table name: %s' % table_id)
        project, dataset, table_name = match.groups()
        table = bigquery.Client(project=project).dataset(dataset).table(table_name)
        table.reload()
        return schema_from_bigquery_schema(table.schema)

    def _table(self, table_name, *args, **kwargs):
        return self._bigquery_client.dataset(self.dataset).table(
            table_name, *args, **kwargs)


        while query.job.state != 'DONE':
            query.job.reload()


# until google fixes https://github.com/GoogleCloudPlatform/google-cloud-python/issues/2354
def query_fetch_data(client, query):
    path = '/projects/%s/queries/%s' % (query.project, query.name)
    params = {}
    response = client.connection.api_request(
        method='GET', path=path, query_params=params)

    query._set_properties(response)
