## sqlleaf
Extract detailed column-level lineage and SQL functions from SQL statements and stored procedures.

This extends the wonderful open source project `sqlglot` and its built-in `lineage.lineage()` function with the following features:
- detailed column-level lineage with variables, literals and functions as column sources
- extraction of SQL functions - their names, arguments and positions
- parsing of stored procedures

## Quickstart
Define your SQL tables:
```python
tables = '''
CREATE TABLE fruit.raw (
    kind VARCHAR
);

CREATE TABLE fruit.processed (
    amount INT
);
'''
import sqlleaf
mapping = sqlleaf.create_schema_mapping(text=tables, dialect='redshift')
```

Produce lineage from the query:
```python
query = '''
INSERT INTO fruit.processed
SELECT
    COUNT(kind) AS amount
FROM fruit.raw;
'''
lineage = sqlleaf.get_lineage_from_sql(text=query, dialect='redshift', mapping=mapping)
print(lineage.to_json())
```

```json
[
  {
    "parent": {
      "schema": "fruit",
      "table": "raw",
      "column": "kind",
      "column_type": "VARCHAR",
      "kind": "column",
      "is_view": false
    },
    "child": {
      "schema": "fruit",
      "table": "processed",
      "column": "amount",
      "column_type": "INT",
      "kind": "column",
      "is_view": false
    },
    "functions": [
      {
        "name": "count",
        "kind": "function",
        "depth": 0,
        "arguments": [
          {
            "value": "fruit.raw.kind",
            "kind": "column",
            "position": 0,
            "parent_path": true
          }
        ]
      }
    ]
  }
]
```

## Leaf-based lineage
Tools that rely on column-level lineage skip information about non-column sources of information. We often need to know
all the sources of data for a particular column.

For example, consider the SQL snippet:
```sql
INSERT INTO fruit.processed
SELECT
    CASE WHEN age < 2 THEN 'new' ELSE 'old' END AS kind
FROM fruit.raw
```
Notice how none of the columns from `fruit.raw` have their data inserted into the `fruit.processed` columns; only the literal values `new` or `old` are selected.\
Existing column-level lineage tools would skip this information, leaving out an explanation for the source of the data.\
In order to construct the complete lineage for each column, we need to track the non-column sources of these types of expressions.

The types of leaves that are available are: `column`, `function`, `literal` and `variable`.

## Function search
You can extract the functions used inside an SQL statement. For example,
```python
# Get edges without functions (e.g. for foreign keys)
graph = lineage.get_edges_without_functions()

# Get edges containing the function `count()`
graph = lineage.get_edges_containing_function(name='count')

# Get edges containing the function `lower(my.column)`
graph = lineage.get_edges_containing_function(name='lower', arguments={'kind': 'column', 'value': 'my.column'})

# Get edges containing the function `etl.create_key(my.column, 'USD')` with `USD` as the second parameter
graph = lineage.get_edges_containing_function(name='etl.create_key', arguments={'kind': 'literal', 'value': 'USD', 'position': 1})
```
These all return `network` MultiDiGraphs, which are directed graphs allowing multiple edges between two nodes.

If you are not familiar with `networkx`, you can print the above like so:
```python
for n1, n2, data in graph.edges.data():
    print(json.dumps(data['attrs'].get_attributes()), indent=2))
```
```json
{
  "parent": "fruit.raw.kind",
  "parent_type": "VARCHAR",
  "parent_kind": "column",
  "child": "fruit.processed.amount",
  "child_type": "INT",
  "child_kind": "column",
  "functions": ["count"],
  "stored_procedure": "",
  "statement_idx": 0,
  "selected_idx": 0,
  "path_idx": 0
}
```

## Function extraction
You can extract functions from the SQL statements:
```python
for n1, n2, data in lineage.iter_edges():
    print('%s -> %s [%s]', (n1, n2, data['attrs'].get_function_names()))
```
```text
fruit.raw.kind -> fruit.raw.processed [count]
```

## A more complex example
This example parses a stored procedure
```python
tables = '''
CREATE TABLE fruit.raw (
    kind VARCHAR
);

CREATE TABLE fruit.processed (
    amount INT,
    number INT,
    kind VARCHAR
);
'''

import sqlleaf
dialect = 'redshift'
mapping = sqlleaf.create_schema_mapping(text=tables, dialect=dialect)

query = '''
CREATE OR REPLACE PROCEDURE fruit.process(v_kind VARCHAR, v_amount INT)
	LANGUAGE plpgsql
	SECURITY DEFINER
AS $$

DECLARE
    v_name VARCHAR;

BEGIN

    INSERT INTO fruit.processed
    WITH cte AS ( 
        SELECT upper(kind) AS knd
        FROM fruit.raw
    )
    SELECT 
        v_amount as amount,
        1 as number,
        lower(c.knd) as kind
    FROM cte c;
    
EXCEPTION WHEN OTHERS THEN
    SELECT 1;
END;
$$;
'''

lineage = sqlleaf.get_lineage_from_procedure(text=query, dialect='redshift', mapping=mapping)
print(lineage.to_json())
```
Ouput:
```json
[
  {
    "parent": {
      "schema": "",
      "table": "",
      "column": "v_amount",
      "column_type": "INT",
      "kind": "variable",
      "is_view": false
    },
    "child": {
      "schema": "fruit",
      "table": "processed",
      "column": "amount",
      "column_type": "INT",
      "kind": "column",
      "is_view": false
    },
    "functions": []
  },
  {
    "parent": {
      "schema": "",
      "table": "",
      "column": "1",
      "column_type": "INT",
      "kind": "literal",
      "is_view": false
    },
    "child": {
      "schema": "fruit",
      "table": "processed",
      "column": "number",
      "column_type": "INT",
      "kind": "column",
      "is_view": false
    },
    "functions": []
  },
  {
    "parent": {
      "schema": "fruit",
      "table": "raw",
      "column": "kind",
      "column_type": "VARCHAR",
      "kind": "column",
      "is_view": false
    },
    "child": {
      "schema": "fruit",
      "table": "processed",
      "column": "kind",
      "column_type": "VARCHAR",
      "kind": "column",
      "is_view": false
    },
    "functions": [
      {
        "name": "upper",
        "kind": "function",
        "depth": 0,
        "arguments": [
          {
            "value": "fruit.raw.kind",
            "kind": "column",
            "position": 0,
            "parent_path": true
          }
        ]
      },
      {
        "name": "lower",
        "kind": "function",
        "depth": 1,
        "arguments": [
          {
            "value": "upper",
            "kind": "function",
            "position": 0,
            "parent_path": true
          }
        ]
      }
    ]
  }
]
```

## Limitations
- UPDATE statement are not yet supported

## Upcoming features
- warnings of invalid insertions between column types
- warnings of unused columns
