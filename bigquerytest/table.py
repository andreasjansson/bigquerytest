import json
import base64
import hashlib
import re
from collections import namedtuple
from google.cloud import bigquery


class Null(object):
    pass


def or_null(f):
    def wrapped(s):
        if s == 'null':
            return Null()
        return f(s)
    return wrapped


PRIMITIVE_CONVERTERS = {
    'string': or_null(lambda s: s),
    'integer': or_null(int),
    'boolean': or_null(lambda s: s.lower().startswith('t')),
    'float': or_null(float),
}

SUPPORTED_TYPES = set(PRIMITIVE_CONVERTERS) | set(["record"])


class BigQueryTestTable(object):

    def __init__(self, data, schema):
        self.data = data
        self.schema = schema

    def prettyprint(self, column_widths=None, min_space=2):
        return table_prettyprint(self.flatten(), column_widths, min_space)

    def flatten(self):
        return flatten_table(self.data, self.schema)

    def get_column_names(self):
        return columns_from_schema(self.schema)

    def get_hash(self):
        m = hashlib.md5()
        m.update(json.dumps(self.data, sort_keys=True) +
                 json.dumps(self.schema, sort_keys=True))
        return re.sub('[^a-zA-Z0-9]', '', base64.b64encode(m.hexdigest()))[:20]

    def get_bigquery_schema(self):
        return bigquery_schema_from_schema(self.schema)


BigQueryTestSchemaField = namedtuple('BigQueryTestSchemaField', 'name type long_name subfields nullable repeated is_repeated_branch')


def schema_from_bigquery_schema(bq_fields, prefix='', is_repeated_branch=False):
    return [
        BigQueryTestSchemaField(
            f.name,
            f.field_type.lower(),
            prefix + f.name,
            schema_from_bigquery_schema(f.fields, prefix + f.name + '.', is_repeated_branch or f.mode == 'REPEATED') if f.fields else None,
            f.mode == 'NULLABLE',
            f.mode == 'REPEATED',
            is_repeated_branch or f.mode == 'REPEATED'
        )
        for f in bq_fields
    ]


def bigquery_schema_from_schema(fields):
    return [
        bigquery.table.SchemaField(
            f.name,
            f.type.upper(),
            mode='NULLABLE' if f.nullable else 'REPEATED' if f.repeated else 'REQUIRED',
            fields=bigquery_schema_from_schema(f.subfields) if f.subfields else None
        )
        for f in fields
    ]


def table_from_executed_query(query):
    # until google fixes https://github.com/GoogleCloudPlatform/google-cloud-python/issues/2354
    response = query.__dict__['_properties']['rows']
    schema = schema_from_bigquery_schema(query.schema)
    return table_from_api_response(response, schema)


def table_from_api_response(response, schema):
    records = []
    for row in response:
        records.append(parse_api_response_fields(row['f'], schema))
    return BigQueryTestTable(records, schema)


def parse_api_response_fields(record_fields, schema_fields):
    return {
        field.name: parse_api_response_value(value['v'], field)
        for field, value in zip(schema_fields, record_fields)
        if value['v']
    }


def parse_api_response_value(value, field):
    if field.type in PRIMITIVE_CONVERTERS:
        return PRIMITIVE_CONVERTERS[field.type](value)
    elif field.type == 'record':
        if field.repeated:
            return [parse_api_response_fields(v['v']['f'], field.subfields)
                    for v in value]
        else:
            return parse_api_response_fields(value['f'], field.subfields)
    else:
        raise ValueError('Unsupported data type: %s' % field.type)


def table_from_definition_string(table_definition, schema):
    columns = None
    records = []
    record = None
    for row in table_definition.strip('\n').splitlines():

        # comments start with #, empty lines are ignored
        if row.strip().startswith('#') or not row.strip():
            continue

        if '\t' in row:
            raise ValueError('Please use spaces instead of tabs')

        if columns is None:
            columns, offsets = parse_column_header(row)
            schema = narrow_fields_to_columns(schema, columns)
        else:
            parsed_row = parse_row(row, columns, offsets)
            if is_new_record(schema, parsed_row):
                if record is not None:
                    records.append(record)
                record = {}

            update_record(schema, parsed_row, record)

    if record:
        records.append(record)

    return BigQueryTestTable(records, schema)


def narrow_fields_to_columns(fields, columns):
    narrow = []
    for field in fields:
        narrow += narrow_field_to_columns(field, columns)
    return narrow


def narrow_field_to_columns(field, columns):
    if field.type == 'record':
        narrow_subfields = narrow_fields_to_columns(field.subfields, columns)
        if narrow_subfields:
            return [BigQueryTestSchemaField(
                field.name,
                field.type,
                field.long_name,
                narrow_subfields,
                field.nullable,
                field.repeated,
                field.is_repeated_branch)]

    if field.long_name in columns:
        return [field]

    return []


def parse_column_header(row):
    columns = []
    offsets = []
    for match in re.finditer(r'[^ ]+', row):
        columns.append(match.group())
        offsets.append(match.span()[0])

    return columns, offsets


def parse_row(row, columns, offsets):
    parsed_row = {}

    header = ''
    for column, offset in zip(columns, offsets):
        header += ' ' * (offset - len(header)) + column

    for i, (column, start) in enumerate(zip(columns, offsets)):

        if start >= len(row):
            break

        if i == len(offsets) - 1:
            value = row[start:len(row)]
        else:
            value = row[start:offsets[i + 1]]
            if len(row) > offsets[i + 1] and value[-1] != ' ':
                raise ValueError('Values do not line up with columns: \n%s\n%s' % (header, row))

        if value[0] == ' ' and value.strip():
            raise ValueError('Values do not line up with columns: \n%s\n%s' % (header, row))

        value = value.strip()
        if value:
            parsed_row[column] = value

    return parsed_row


def is_new_record(schema, parsed_row):
    return all([parsed_row.get(f.long_name)
                for f in schema_leaves(schema)
                if f.type != 'record'
                #if not f.nullable
                and not f.is_repeated_branch
    ])


def update_record(schema, parsed_row, record):
    for field in schema:
        update_record_for_field(field, parsed_row, record)


def update_record_for_field(field, parsed_row, record):
    if field.type not in SUPPORTED_TYPES:
        raise ValueError('Field type not supported: %s' % field.type)

    value = None
    append = True
    if field.long_name in parsed_row:
        if field.type in PRIMITIVE_CONVERTERS:
            value = PRIMITIVE_CONVERTERS[field.type](parsed_row[field.long_name])

    if field.type == 'record':
        if field.repeated:
            if is_new_record(field.subfields, parsed_row) or not record.get(field.name):
                value = {}
            else:
                value = record[field.name][-1]
                append = False
        else:
            value = record.get(field.name, {})
        for f in field.subfields:
            update_record_for_field(f, parsed_row, value)

    if value:
        if field.repeated:
            if append:
                record[field.name] = record.get(field.name, []) + [value]
            else:
                record[field.name][-1] = value
        else:
            record[field.name] = value


def schema_leaves(fields):
    leaves = []
    for field in fields:
        if field.type == 'record':
            leaves += schema_leaves(field.subfields)
        else:
            leaves.append(field)
    return leaves


def columns_from_schema(fields):
    return [field.long_name for field in schema_leaves(fields)]


def flatten_record(record, field, prefix, column_map, flat, index):
    if isinstance(record, list):
        for item in record:
            index = max(flatten_record(item, field, prefix, column_map, flat, index), index + 1)
        return index
    else:
        if field.type == 'record':
            return flatten_composite_record(record, field.subfields,
                                     prefix, column_map, flat, index)
        else:
            if len(flat) <= index:
                flat.append([None] * len(column_map))
            column_index = column_map[field.long_name]
            flat[index][column_index] = record
            return index


def flatten_composite_record(record, fields, prefix, column_map, flat, index):
    indexes = []
    for f in fields:
        if f.name in record:
            indexes.append(
                flatten_record(record[f.name], f,
                               prefix + f.name + '.', column_map,
                               flat, index))

    return max(indexes)


def flatten_table(table, schema):
    flat = []
    columns = columns_from_schema(schema)
    column_map = {c: i for i, c in enumerate(columns)}
    flat.append(columns)

    index = 1
    for record in table:
        index = max(flatten_composite_record(record, schema, '', column_map,
                                             flat, index), index + 1)
    return flat


def get_column_widths(flat):
    widths = [len(s) for s in flat[0]]
    for row in flat[1:]:
        for i, s in enumerate(row):
            if s and len('%s' % s) > widths[i]:
                widths[i] = len('%s' % s)
    return widths


def table_prettyprint(flat, column_widths=None, min_space=2):
    if column_widths is None:
        column_widths = get_column_widths(flat)

    rows = []
    for row in flat:
        rows.append((' ' * min_space).join([
            ('%%-%ds' % w) % (s if s is not None else '')
            for w, s in zip(column_widths, row)]).rstrip())

    return '\n'.join(rows)
