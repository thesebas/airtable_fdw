import datetime
import json
import logging
from typing import Any

from airtable import Airtable

from airtable_fdw.utils import quals_to_formula
from multicorn import ForeignDataWrapper, ColumnDefinition
from multicorn.utils import log_to_postgres as log

__all__ = ['AirtableFDW']

log("AirtableFDW imported", logging.INFO)


def date_datetime(value: [datetime.datetime, str]) -> str:
    if isinstance(value, datetime.date):
        return value.strftime('%Y-%m-%d')
    return value


AIRTABLE_TYPES_MAP = {
    'json': lambda value: json.dumps(value)
}
PG_TYPES_MAP = {
    'date': date_datetime
}


def convert_at_to_pg(column_definition: ColumnDefinition, value: Any) -> Any:
    return AIRTABLE_TYPES_MAP.get(column_definition.base_type_name, lambda x: x)(value)


def convert_pg_to_at(column_definition: ColumnDefinition, value: Any) -> Any:
    return PG_TYPES_MAP.get(column_definition.base_type_name, lambda x: x)(value)


TECHNICAL_COLUMNS = ['oid', 'ctid', 'tableoid']


class AirtableFDW(ForeignDataWrapper):
    __slots__ = ['airtable', 'columns', 'options', 'view_name', 'update_batch', 'insert_batch', 'delete_batch']

    def __init__(self, fdw_options, fdw_columns):
        super().__init__(fdw_options, fdw_columns)

        log("AirtableFDW::init(%s, %s)" % (repr(fdw_options), repr(fdw_columns)), logging.INFO)

        base_key = fdw_options.get('base_key')
        api_key = fdw_options.get('api_key')
        table_name = fdw_options.get('table_name')

        self.view_name = fdw_options.get('view_name', None)
        self._rowid_column = fdw_options.get('rowid_column', None)

        self.airtable = Airtable(
            base_key=base_key,
            table_name=table_name,
            api_key=api_key,
        )

        self.columns = fdw_columns
        self.options = fdw_options

        self.update_batch = []
        self.insert_batch = []
        self.delete_batch = []

    def execute(self, quals, columns, sortkeys=None):
        log("Airtable::execute(%s, %s, %s)" % (quals, columns, sortkeys), logging.INFO)

        fields = [self.columns[key].column_name for key in columns if key not in TECHNICAL_COLUMNS and key != self._rowid_column]

        filter_formula = quals_to_formula(quals)
        log("Airtable::execute formula = %s" % filter_formula, logging.DEBUG)

        sort_fields = [(sortkey.attname, 'desc' if sortkey.is_reversed else 'asc') for sortkey in sortkeys] if sortkeys is not None else []
        log("Airtable::execute sort = %s" % sort_fields, logging.DEBUG)

        batches = self.airtable.get_iter(fields=fields, formula=filter_formula, sort=sort_fields, view=self.view_name)

        for batch_idx, batch in enumerate(batches):
            log("Airtable::execute batch = %s" % batch_idx, logging.INFO)
            for record in batch:
                # yield {**record['fields'], 'id': record['id']}
                row = {column: convert_at_to_pg(self.columns.get(column), record.get('fields', {}).get(column, None)) for column in columns if column not in ['ctid', 'tableoid', 'oid', 'id']}
                row.update({self._rowid_column: record.get('id')})

                log("Airtable::execute row = %s" % row, logging.DEBUG)
                yield row

    def end_scan(self):
        log('Airtable::end_scan()', logging.INFO)

    def can_sort(self, sortkeys):
        log('Airtable::can_sort(%s)' % repr(sortkeys))
        return [sortkey for sortkey in sortkeys]

    def insert(self, values):
        log('Airtable::insert(%s)' % (values,), logging.INFO)
        fields = {name: convert_pg_to_at(self.columns.get(name), value) for name, value in values.items()}

        self.update_batch.append(fields)

    def update(self, rowid, newvalues):
        log('Airtable::update(%s, %s)' % (rowid, newvalues), logging.INFO)

        fields = {name: convert_pg_to_at(self.columns.get(name), value) for name, value in newvalues.items()}
        self.update_batch.append(dict(id=rowid, fields=fields))

    def delete(self, rowid):
        log('Airtable::delete(%s)' % (rowid,), logging.INFO)
        self.delete_batch.append(rowid)

    def end_modify(self):
        log(
            'Airtable::end_modify() - update_count %d, insert_count %d, delete_count %d' %
            (len(self.update_batch), len(self.insert_batch), len(self.delete_batch)),
            logging.INFO
        )
        log('Airtable::end_modify() - updates: %s' % repr(self.update_batch), logging.DEBUG)
        log('Airtable::end_modify() - inserts: %s' % repr(self.insert_batch), logging.DEBUG)
        log('Airtable::end_modify() - deletes: %s' % repr(self.delete_batch), logging.DEBUG)

        try:
            self.airtable.batch_update(self.update_batch, True)
            self.update_batch.clear()
        except Exception as e:
            log('failed to update - %s' % e, logging.WARNING)

        try:
            self.airtable.batch_insert(self.insert_batch, True)
            self.insert_batch.clear()
        except Exception as e:
            log('failed to insert - %s' % e, logging.WARNING)

        try:
            self.airtable.batch_delete(self.delete_batch)
            self.delete_batch.clear()
        except Exception as e:
            log('failed to delete - %s' % e, logging.WARNING)

    # @classmethod
    # def import_schema(self, schema, srv_options, options, restriction_type, restricts):
    #     pass

    @property
    def rowid_column(self):
        log('Airtable::rowid_column()', logging.INFO)
        return self._rowid_column
