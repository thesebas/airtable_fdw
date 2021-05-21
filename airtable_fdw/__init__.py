import datetime
import json
import logging
from typing import Any, Dict, List, Iterator

from airtable import Airtable

from airtable_fdw.utils import quals_to_formula, first
from multicorn import ForeignDataWrapper, ColumnDefinition, Qual, SortKey
from multicorn.utils import log_to_postgres as log

__all__ = ['AirtableFDW']

log("AirtableFDW imported", logging.INFO)


def date_datetime(column_definition: ColumnDefinition, value: [datetime.datetime, str]) -> str:
    if isinstance(value, datetime.date):
        return value.strftime('%Y-%m-%d')
    return value


def complextype_dict_to_record(column_definition: ColumnDefinition, value: Dict[str, Any]) -> str:
    if value is None:
        return None
    fields_order = column_definition.options.get('complextype_fields', '').split(',')
    return "(%s)" % ",".join([value.get(field_name, None) for field_name in fields_order])


def complextype_recordstr_to_value(column_definition: ColumnDefinition, value: str) -> Any:
    if value is None:
        return None;
    fields_order = column_definition.options.get('complextype_fields', '').split(',')
    values = value[1:-1].split(',')

    d = dict(zip(fields_order, values))
    ret = d.get(column_definition.options.get('complextype_send'), None)
    log('complextype_recordstr_to_value: %s[%s]=%s' % (d, column_definition.options.get('complextype_send'), ret), logging.DEBUG)
    return ret


def quals_contains_get_by_rowid(rowid, quals: List[Qual]) -> bool:
    for qual in quals:
        if qual.field_name == rowid:
            if not qual.is_list_operator and qual.operator == '=':
                return True
            else:
                if qual.operator[0] == '=':
                    return True

    return False


def extract_rowids_from_quals(rowid, quals: List[Qual]) -> List:
    return [
        qual.value for qual in quals if qual.field_name == rowid and ((not qual.is_list_operator and qual.operator == '=') or (qual.is_list_operator and qual.operator[0] == '='))
    ]


AIRTABLE_TYPES_MAP = {
    'json': lambda column_definition, value: None if value is None and 'nulljson' not in column_definition.options else json.dumps(value)

}

PG_TYPES_MAP = {
    'date': date_datetime
}


def convert_at_to_pg(column_definition: ColumnDefinition, value: Any) -> Any:
    if 'complextype_fields' in column_definition.options:
        return complextype_dict_to_record(column_definition, value)
    return AIRTABLE_TYPES_MAP.get(column_definition.base_type_name, lambda _, x: x)(column_definition, value)


def convert_pg_to_at(column_definition: ColumnDefinition, value: Any) -> Any:
    if 'complextype_fields' in column_definition.options and 'complextype_send' in column_definition.options:
        return complextype_recordstr_to_value(column_definition, value)
    return PG_TYPES_MAP.get(column_definition.base_type_name, lambda _, x: x)(column_definition, value)


TECHNICAL_COLUMNS = ['oid', 'ctid', 'tableoid']


class AirtableFDW(ForeignDataWrapper):
    __slots__ = ['airtable', 'columns', 'options', 'view_name', 'update_batch', 'insert_batch', 'delete_batch', 'computed_fields']

    def __init__(self, fdw_options: Dict, fdw_columns: Dict[str, ColumnDefinition]):
        super().__init__(fdw_options, fdw_columns)

        log("AirtableFDW::init(%s, %s)" % (repr(fdw_options), repr(fdw_columns)), logging.INFO)

        base_key = fdw_options.get('base_key')
        api_key = fdw_options.get('api_key')
        table_name = fdw_options.get('table_name')

        self.view_name = fdw_options.get('view_name', None)

        rowid_column = first(fdw_columns.values(), lambda definition: 'rowid' in definition.options)

        if rowid_column is not None:
            self._rowid_column = rowid_column.column_name
            log("AirtableFDW::init - 'rowid' column = %s" % self._rowid_column, logging.INFO)
        else:
            rowid_column = fdw_options.get('rowid_column', None)
            if rowid_column is not None:
                if rowid_column not in fdw_columns:
                    log("AirtableFDW::init - invalid 'rowid_column' option, modify operations not possible", logging.WARNING)
                else:
                    self._rowid_column = rowid_column
                    log("AirtableFDW::init - 'rowid' column = %s" % self._rowid_column, logging.INFO)
            else:
                log("AirtableFDW::init - 'rowid' column not defined, modify operations not possible", logging.WARNING)

        self.computed_fields = [definition.column_name for definition in fdw_columns.values() if 'computed' in definition.options]
        log("AirtableFDW::init - computed fields = %s " % self.computed_fields, logging.DEBUG)

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

    def execute(self, quals: List[Qual], columns: Dict, sortkeys: List[SortKey] = None) -> Iterator[Dict[str, Any]]:
        log("Airtable::execute(%s, %s, %s)" % (quals, columns, sortkeys), logging.INFO)

        fields = [self.columns[key].column_name for key in columns if key not in TECHNICAL_COLUMNS and key != self._rowid_column]

        if quals_contains_get_by_rowid(self._rowid_column, quals):

            rowids = extract_rowids_from_quals(self._rowid_column, quals)
            log("Airtable::execute - get records by recordids - %s" % rowids, logging.INFO)
            records = (self.airtable.get(rowid) for rowid in rowids)
        else:

            quals_without_rowid = [qual for qual in quals if qual.field_name != self._rowid_column]
            filter_formula = quals_to_formula(quals_without_rowid)
            log("Airtable::execute - get records by formula = %s" % filter_formula, logging.DEBUG)

            sort_fields = [(sortkey.attname, 'desc' if sortkey.is_reversed else 'asc') for sortkey in sortkeys] if sortkeys is not None else []
            log("Airtable::execute sort = %s" % sort_fields, logging.DEBUG)

            batches = self.airtable.get_iter(fields=fields, formula=filter_formula, sort=sort_fields, view=self.view_name)

            records = (row for batch in batches for row in batch)

        for record in records:
            row = {
                column: convert_at_to_pg(self.columns.get(column), record.get('fields', {}).get(column, None))
                for column in columns
                if column not in ['ctid', 'tableoid', 'oid', 'id']
            }
            row.update({self._rowid_column: record.get('id')})

            log("Airtable::execute row = %s" % row, logging.DEBUG)
            yield row

    def end_scan(self):
        log('Airtable::end_scan()', logging.INFO)

    def can_sort(self, sortkeys: List[SortKey]) -> List[SortKey]:
        log('Airtable::can_sort(%s)' % repr(sortkeys))
        return [sortkey for sortkey in sortkeys]

    def insert(self, values: Dict[str, Any]):
        log('Airtable::insert(%s)' % (values,), logging.INFO)

        fields = {
            name: convert_pg_to_at(self.columns.get(name), value)
            for name, value in values.items()
            if name not in self.computed_fields and name != self._rowid_column
        }

        self.insert_batch.append(fields)

    def update(self, rowid: Any, newvalues: Dict[str, Any]):
        log('Airtable::update(%s, %s)' % (rowid, newvalues), logging.INFO)

        fields = {
            name: convert_pg_to_at(self.columns.get(name), value)
            for name, value in newvalues.items()
            if name not in self.computed_fields and name != self._rowid_column
        }

        self.update_batch.append(dict(id=rowid, fields=fields))

    def delete(self, rowid: Any):
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
        except Exception as e:
            log('failed to update - %s' % e, logging.WARNING)
        finally:
            self.update_batch.clear()

        try:
            self.airtable.batch_insert(self.insert_batch, True)
        except Exception as e:
            log('failed to insert - %s' % e, logging.WARNING)
        finally:
            self.insert_batch.clear()

        try:
            self.airtable.batch_delete(self.delete_batch)
        except Exception as e:
            log('failed to delete - %s' % e, logging.WARNING)
        finally:
            self.delete_batch.clear()

    # @classmethod
    # def import_schema(self, schema, srv_options, options, restriction_type, restricts):
    #     pass

    @property
    def rowid_column(self) -> str:
        if self._rowid_column is None:
            raise NotImplementedError("This FDW does not support the writable API")

        log('Airtable::rowid_column()', logging.INFO)
        return self._rowid_column

    # test3
