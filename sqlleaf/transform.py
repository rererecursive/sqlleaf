import typing as t
import copy

import sqlglot
from sqlglot import exp

from sqlleaf import exception

def apply_optimizations(statement: exp.Expression, dialect: str, mapping, path, child_table):
    """
    1. We pass validate=false to prevent errors like: sqlglot.errors.OptimizeError: Column '"v_ca_start_date_id"' could not be resolved
    2. We pass infer_schema=True to source unqualified columns from the source table (if missing from the `schema` param)
        e.g. so that
            INSERT INTO my.other SELECT name FROM my.table
        produces
            my.table.name -> my.other.name
    """
    try:
        stmt = sqlglot.optimizer.qualify.qualify(statement, schema=mapping, infer_schema=True, dialect=dialect, isolate_tables=False, validate_qualify_columns=False, quote_identifiers=False)
    except sqlglot.errors.OptimizeError as e:
        raise exception.SqlGlotException(message=str(e), filename=path, table=child_table)

    stmt = add_aliases_to_selects(stmt, path, child_table)
    stmt = sqlglot.optimizer.annotate_types.annotate_types(stmt, dialect=dialect, schema=mapping)

    return stmt


def add_aliases_to_selects(statement, path: str, child_table):
    """
    Add aliases to SELECTs that are missing them by looking at the corresponding INSERT column.
    This prevents sqlglot from assigning its own generated names as aliases.

    For example, the statement:
        INSERT INTO my.apple (a,b) SELECT name, age FROM my.pear
    renames to:
        INSERT INTO my.apple (a,b) SELECT name as a, age as b FROM my.pear
    """
    insert_columns = statement.this.expressions
    if len(insert_columns) > 0 and len(insert_columns) != len(statement.selects):
        message = f'Mismatched column count: inserted columns (%s) do not match selected columns (%s)' % (len(insert_columns), len(statement.selects))
        raise exception.SqlGlotException(message=message, filename=path, table=child_table)

    for i, ins in enumerate(insert_columns):
        statement.selects[i] = statement.selects[i].as_(ins)
    return statement


def case_statement_transformer(node):
    """
    Transform the 'WHEN' part of every CASE statement so be 1=1 so that the lineage
    does not include the original columns in this clause.
    sqlglot will include columns in 'WHEN' to the lineage by default, but they're really false positives.

    This is quite hacky, but it's the cleanest approach considering the limitations of sqlglot's Scope() and
    lineage() functions.
    """
    if isinstance(node, exp.Case):
        case = exp.case()
        for _if in node.args['ifs']:
            try:
                case = case.when('1=1', then=_if.args['true'])
                if 'default' in node.args:
                    case = case.else_(node.args['default'])
            except Exception as e:
                pass
        return case
    return node


def remove_lines_before_begin(lines: t.List[str]) -> t.List[str]:
    """
    Remove every line until 'BEGIN', inclusive.
    """
    stripped_lines = [line.lower().strip() for line in lines]

    # Only process files that contain 'begin'
    if "begin" not in stripped_lines:
        return lines

    new_lines = copy.copy(lines)

    for i, line in enumerate(lines):
        l = line.lower().strip()
        if not l.startswith("--"):
            line = "-- " + line

        # Only overwrite/strip new lines
        new_lines[i] = line
        if l == "begin":
            break

    return new_lines


def remove_lines_after_exception(lines: t.List[str]) -> t.List[str]:
    """
    Remove every line after 'EXCEPTION', inclusive.
    """
    new_lines = []

    for i, line in enumerate(lines):
        if line.lower().strip().startswith('exception'):
            break
        new_lines.append(line)

    return new_lines


def remove_raise_statements(lines: t.List[str]) -> t.List[str]:
    """
    Remove every line starting with 'RAISE'.
    """
    new_lines = []

    for i, line in enumerate(lines):
        if line.lower().strip().startswith('raise '):
            continue
        new_lines.append(line)

    return new_lines


def clean_stored_procedure_file(text: str):
    """
    Extract the inner queries from inside the stored procedure by removing any syntax/keywords that cannot be processed
    by `sqlglot`.
    """
    print(f"Cleaning and generating file.")
    lines = text.splitlines()

    # Transform the file's text
    lines = remove_lines_before_begin(lines)
    lines = remove_lines_after_exception(lines)
    lines = remove_raise_statements(lines)

    return '\n'.join(lines)
