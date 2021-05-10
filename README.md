# Airtable Foreign Data Wrapper

## Usage

Ensure multicorn is loaded and define Foreign Data Wrapper for airtable

```postgresql
create extension if not exists multicorn;
create server if not exists multicorn_airtable_srv foreign data wrapper multicorn options (
    wrapper 'airtable_fdw.AirtableFDW'
    );
```

Define table as

```postgresql
create foreign table schema.table_name (
    "_id" varchar options (rowid 'true'),                       -- column used as rowid, may be any name, 
                                                                -- should appear only onece
    "Some text column" varchar,
    "Some numeric column" numeric,
    "Some date column" date,
    "Some complex column" json,                                 -- can be used for complex fields but see example below 
    "Some json nullable column" json options (nulljson 'true'), -- keep nulls as json ('null'::json instead of null::json)
    "Some computed column" varchar options (computed 'true')    -- column that won't be modified with update
                                                                -- may appear multiple times
) server multicorn_airtable_srv options (
    api_key '...',      -- api access key
    base_key '...',     -- database identifier
    table_name '...',   -- name of table to read from
    view_name '...',    -- optional view name, if not present raw table will be read
    rowid_column '...'  -- optional rowid column name will be used if no column has `rowid` option set 
);
```

If complex column - like `Collaborator` - appears in table it is read from AirTable API as a `json` and could be treated as `json` or as a complex, custom defined type.

```postgresql
create type AirtableCollaborator as
(
    id     varchar,
    email  varchar,
    "name" varchar
);
create foreign table schema.table_name (
    "_id" varchar options (rowid 'true'),
    "editor" AirtableCollaborator options (complextype_fields 'id,email,name', complextype_send 'email')
) server multicorn_airtable_srv options (
    api_key '...',
    base_key '...',
    table_name '...'
);

```

where:
* `complextype_fields 'id,email,name'` indicates how record string should be constructed from `json` - so `{"id": "someid", "email": "me@example.com", "name":"My Name"}` will be converted to `(someid,me@example.com,My Name)` and will be correctly casted to `AirtableCollaborator` type.
* `complextype_send 'email'` means that when this field is modified only `email` field will be sent to API

## Features

* Configurable to read from given base / table / view
* SQL `WHERE` clause transformed to `formula` query (so number of requests to API is optimized)
* Batch `INSERT`/`UPDATE`/`DELETE`
* support for complex types - json is parsed to complex type on read (`SELECT`), and single, selected field is set on write (`INSERT`, `UPDATE`) 

## Usage Tips

* Use `AND` in `WHERE` clause whenever possible, `OR`s are not handled well (at all?) by *multicorn* so unconditional queries are sent to Airtable (watch the quota!).
* If `OR` is required try to replace it with  `IN (...)`