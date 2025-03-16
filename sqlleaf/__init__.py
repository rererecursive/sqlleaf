import logging

logging.basicConfig(level=logging.NOTSET)

logger = logging.getLogger("sqlleaf")
logger.setLevel(logging.DEBUG)

import networkx as nx
import sqlglot
from sqlglot import exp

from sqlleaf import structs, exception, validate, transform, graph, lineage


def create_schema_mapping(text: str, dialect: str) -> sqlglot.MappingSchema:
    """
    Convert a series of `CREATE TABLE ...` statements into sqlglot's MappingSchema.

    Parameters:
        text (str): the text containing the CREATE TABLE statements
        dialect (str): the SQL dialect
        clean_file (bool): whether to clean the file
    """
    logger.debug('Creating schema mapping of tables/views.')
    objects = 0
    mapping = sqlglot.MappingSchema(dialect=dialect, normalize=False)
    parsed = sqlglot.parse(text, dialect=dialect)

    for stmt in parsed:
        # Extract the columns into the mapping
        if isinstance(stmt, exp.Create):
            if stmt.kind == 'TABLE':
                table = stmt.this.this
                columns = {c.name: str(c.kind) for c in stmt.find_all(exp.ColumnDef)}
            elif stmt.kind == 'VIEW':
                table = stmt.this
                columns = {c.name: 'UNKNOWN' for c in stmt.find_all(exp.Column)}
            else:
                continue
            mapping.add_table(table=table, column_mapping=columns)
            objects += 1
        else:
            logger.debug(f'Skipping object since it is not a table: {type(stmt)}')

    logger.debug('Found %s objects.' % (objects,))

    return mapping


def get_lineage_from_procedure(text: str, dialect: str, mapping: sqlglot.MappingSchema, path: str = '') -> structs.LineageHolder:
    """
    Create the lineage for a stored procedure.
    """
    proc_statement = validate.validate_procedure_file(text, dialect)

    metadata = structs.DDLMetadata('PROCEDURE', proc_statement, text, path)
    text = transform.clean_stored_procedure_file(text)
    lineage_holder = get_lineage_from_sql(text, dialect, mapping, metadata, path)

    return lineage_holder


def get_lineage_from_sql(text: str, dialect: str, mapping: sqlglot.MappingSchema, metadata: structs.DDLMetadata=None, path=None) -> structs.LineageHolder:
    """
    Get the column-level lineage for one or more SQL statements.
    """
    try:
        parsed_statements = sqlglot.parse(text, dialect=dialect)
        parsed_statements = [statement for statement in parsed_statements if statement]
    except Exception as e:
        raise exception.SqlGlotException(message=e, filename=path, table='')

    lineage_holder = structs.LineageHolder()

    # Process each of the statements
    for statement_index, statement in enumerate(parsed_statements):
        logger.info(f'Processing parsed statement {statement_index+1}/{len(parsed_statements)} - {str(type(statement))}')

        if isinstance(statement, exp.Command) and statement.this == 'CALL':
            # TODO: add hooks to allow users to include custom logic
            logging.info("Skipping CALL statement.")
            continue

        elif not isinstance(statement, exp.Insert):  # TODO: include Updates; see _extract_select_from_update() in datahub/metadata-ingestion/src/datahub/sql_parsing/sqlglotlineage.py
            continue

        query_type = statement.key

        # "INSERT INTO my.table SELECT ..." vs "INSERT INTO my.table (a,b) SELECT ..."
        child_table = statement.this.this if isinstance(statement.this, exp.Schema) else statement.this

        # Apply sqlglot's optimize() functions
        statement = transform.apply_optimizations(statement, dialect, mapping, path, child_table)

        # Ensure the child table exists with the expected columns
        child_columns = validate.validate_child_table(child_table, mapping, statement, path)

        # Apply sqlglot's simplify() functions
        statement = sqlglot.optimizer.optimizer.simplify(statement)

        # Transform CASE statements
        statement = statement.transform(transform.case_statement_transformer)

        # Get the statement's column-level lineage
        statement_graph = lineage.get_lineage_for_columns(child_columns, child_table, statement, dialect, mapping, metadata, statement_index)

        lineage_holder.add_graph(statement_graph)

    graph.add_path_id_and_root_to_edges(lineage_holder.graph)

    return lineage_holder
