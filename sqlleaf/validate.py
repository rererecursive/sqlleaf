import typing as t

import sqlglot
from sqlglot import exp

from sqlleaf import util, exception


def validate_procedure_file(text: str, dialect: str):
    """
    Ensure only one stored procedure definition exists in the file.
    """
    parsed_stmts_original = sqlglot.parse(text, dialect=dialect)
    proc_stmts = [stmt for stmt in parsed_stmts_original if isinstance(stmt, exp.Create) and stmt.kind == 'PROCEDURE']

    if len(proc_stmts) != 1:
        raise ValueError(f"Error: exactly one stored procedure must be defined per file.")

    return proc_stmts[0]


def validate_child_table(child_table: exp.Table, mapping: sqlglot.MappingSchema, statement: exp.Select, path: str):
    # Ensure that there are no unknown columns
    child_columns: t.Dict[str, str] = mapping.find(child_table) or {}
    if not child_columns:
        raise exception.SqlLeafException(message=f'Unknown table', filename=path, table=str(child_table))

    unknown_columns = util.unique(statement.named_selects - child_columns.keys())

    if unknown_columns:
        raise exception.SqlLeafException(message=f'Unknown columns used in SELECT: {list(unknown_columns)}', filename=path, table=str(child_table))

    if '*' in child_columns.keys():
        raise exception.SqlLeafException(message=f'Statement has unresolved star column', filename=path, table=str(child_table))

    # Use only the query's selected columns (required by sqlglot's lineage())
    child_columns = {col: child_columns[col] for col in statement.named_selects}

    return child_columns
