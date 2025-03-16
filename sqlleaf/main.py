import sqlleaf
from sqlleaf.structs import EdgeAttributes

dialect = 'redshift'
tables = '''
CREATE TABLE fruit.raw (
    kind VARCHAR
);

CREATE TABLE fruit.processed (
    amount INT,
    kind VARCHAR,
    lower_kind VARCHAR,
    count_upper_kind VARCHAR
);

CREATE VIEW fruit.some_view AS 
    SELECT kind FROM fruit.raw;
'''

mapping = sqlleaf.create_schema_mapping(text=tables, dialect=dialect)

query = '''
INSERT INTO fruit.processed
SELECT
    COUNT(kind) AS amount,
    --lower(kind) as lower_kind,
    --count(upper(kind)) as count_upper_kind,
    kind as kind
FROM fruit.view;
'''

lineage_holder = sqlleaf.get_lineage_from_sql(text=query, dialect=dialect, mapping=mapping)

no_functions = lineage_holder.get_edges_without_functions()
print(no_functions.edges())

count_functions = lineage_holder.get_edges_containing_function(name='count')
print(count_functions.edges())

lower_functions = lineage_holder.get_edges_containing_function(name='lower')
print(lower_functions.edges())

upper_functions = lineage_holder.get_edges_containing_function(name='upper')
print(upper_functions.edges())

substring_functions = lineage_holder.get_edges_containing_function(name='substring')
print(substring_functions.edges())

####

print('positions')
count_functions = lineage_holder.get_edges_containing_function(name='count', arguments=[{'kind':'column', 'value': 'fruit.view.kind'}])
print(count_functions.edges())

print('positions2')
count_functions = lineage_holder.get_edges_containing_function(name='count', arguments=[{'kind':'column', 'value': 'fruit.view.kind', 'position': 0}])
print(count_functions.edges())

invalid_inserts = lineage_holder.get_invalid_inserts()
print('invalid inserts:', invalid_inserts.edges())
for n1, n2, data in invalid_inserts.edges.data():
    attrs: EdgeAttributes = data['attrs']
    print('%s [%s] -> %s [%s]' % (n1, attrs.parent.column_type, n2, attrs.child.column_type))

print('positions3')
count_functions = lineage_holder.get_edges_containing_function(name='count', arguments=[{'kind':'column', 'value': 'fruit.view.kind', 'position': 1}])
print(count_functions.edges())

#print(no_functions.edges())
print()
