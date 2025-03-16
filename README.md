## sqlleaf
Extract detailed column-level lineage and SQL functions from SQL statements and stored procedures.

This extends the wonderful open source SQL transpiler [sqlglot](https://github.com/tobymao/sqlglot) with the following features:
- detailed column-level lineage with columns, variables, literals and functions as data sources
- extraction of SQL functions - their names, arguments, positions and ancestry
- parsing of stored procedures

## Quickstart
Define your SQL tables and an SQL query, and then generate the lineage:
```python
import sqlleaf

# Define two tables
tables = '''
CREATE TABLE fruit.raw ( kind VARCHAR );
CREATE TABLE fruit.processed ( amount INT );
'''

# Define the SQL query to insert between them
query = '''
INSERT INTO fruit.processed
SELECT COUNT(kind) AS amount
FROM fruit.raw;
'''

# Create mapping of tables and columns
mapping = sqlleaf.create_schema_mapping(text=tables, dialect='redshift')

# Generate the lineage
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
            "position": 0
          }
        ]
      }
    ]
  }
]
```

The lineage is represented the Python graph library `networkx`:
```python
print('Graph:', lineage.graph)
print('Class:', type(lineage.graph))
```
```
Graph: MultiDiGraph with 2 nodes and 1 edges
Class: <class 'networkx.classes.multidigraph.MultiDiGraph'>
```

Dozens of dialects are supported. For the full list, see the [sqlglot](https://github.com/tobymao/sqlglot) project page.

## Leaf-based lineage
Rather than focusing only on the columns as sources of data in a given lineage, sqlleaf also extracts non-column sources of information. These are named leaves.

For example, consider the SQL snippet:
```sql
INSERT INTO fruit.processed
SELECT
    CASE WHEN age < 2 THEN 'new' ELSE 'old' END AS kind
FROM fruit.raw
```

Other tools would produce lineage:\
`column(fruit.raw.age) -> column(fruit.processed.kind)`

However, we would expect the lineage to be the following:\
`literal("new") -> column(fruit.processed.kind)`\
`literal("old") -> column(fruit.processed.kind)`\
along with the fact that they exist within the context of a `CASE` statement.

This is because none of the columns from `fruit.raw` have their data inserted into the `fruit.processed` columns; only the literal values `new` or `old` are selected.

Thus in order to construct the complete lineage for each column, we must also track the non-column sources of information within the expressions.

The types of leaves that are available are: `column`, `literal`, `function`, and `variable`.

## Function search
You can extract information about the functions used inside an SQL statement. This enables detailed analysis for a wide range of use cases. 

For example, consider the following query:
```sql
INSERT INTO fruit.processed
SELECT 
    name,
    COUNT(kind) AS amount,
    ROUND(etl.calculate_price('USD', cost)) as price
FROM fruit.raw;
```
Assuming we have the lineage created via `sqlleaf.get_lineage_from_sql()`, we can search over the underlying `networkx` graph using the built-in analytical functions.

1. Imagine we need to determine the foreign keys between each table. Let's find the edges that don't have any functions used:
```python
graph = lineage.get_edges_without_functions()
```
We print the graph's edges:
```python
for n1, n2, data in graph.edges.data():
    attrs = data['attrs'].get_attributes()
    print(json.dumps(attrs), indent=2))
```

```json
{
  "parent": "fruit.raw.name",
  "parent_type": "VARCHAR",
  "parent_kind": "column",
  "child": "fruit.processed.name",
  "child_type": "VARCHAR",
  "child_kind": "column",
  "functions": [],
  "stored_procedure": "",
  "statement_idx": 0,
  "selected_idx": 0,
  "path_idx": 0
}
```
Great! This shows us that there is one column that doesn't have a function used, namely `column(fruit.raw.name) -> column(fruit.processed.name)`

---

2. Let's find edges containing the function `COUNT()`:
```python
graph = lineage.get_edges_containing_function(name='count')
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
  "selected_idx": 1,
  "path_idx": 0
}
```
Each of the functions inside the edge attribute `functions` is represented using a Python class that contains useful information for further inspection, such as the function's name, arguments, positioning and its ancestry. See the `OuterFunction` class for full details. 

---

3. Find the edges containing the user-defined function `etl.calculate_price('USD', cost)` and ensure `USD` is the first parameter:
```python
graph = lineage.get_edges_containing_function(name='etl.calculate_price', arguments={'kind': 'literal', 'value': 'USD', 'position': 0})
```
```json
{
  "parent": "fruit.raw.cost",
  "parent_type": "FLOAT",
  "parent_kind": "column",
  "child": "fruit.processed.price",
  "child_type": "FLOAT",
  "child_kind": "column",
  "functions": ["etl.calculate_price", "round"],
  "stored_procedure": "",
  "statement_idx": 0,
  "selected_idx": 2,
  "path_idx": 0
}
```


## A more complex example
This example parses a stored procedure containing a CTE, an input variable and several nested functions:
```python
import sqlleaf

tables = '''
CREATE TABLE fruit.raw ( kind VARCHAR );
CREATE TABLE fruit.processed ( amount INT, number INT, kind VARCHAR );
'''

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
