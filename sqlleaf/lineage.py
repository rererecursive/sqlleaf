from __future__ import annotations
import logging
import typing as t
from dataclasses import dataclass, field

import networkx as nx
import sqlglot
from sqlglot import Schema, exp, maybe_parse
from sqlglot.errors import SqlglotError
from sqlglot.optimizer import Scope, build_scope, find_all_in_scope, normalize_identifiers, qualify, walk_in_scope
from sqlglot.optimizer.scope import ScopeType

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

from sqlleaf import util, structs

logger = logging.getLogger("sqlleaf")

"""
This is a fork of the `sqlglot.lineage` module.
The components mostly still intact are `Node` and `lineage()`.
The rest of the functions are new.
"""


@dataclass(frozen=True)
class Node:
    name: str
    column: exp.Expression  # Usually exp.Column
    expression: exp.Expression
    source: exp.Expression
    downstream: t.List[Node] = field(default_factory=list)
    upstream: t.List[Node] = field(default_factory=list)
    outer_functions: t.List = field(default_factory=list)
    source_name: str = ""
    reference_node_name: str = ""

    def walk(self) -> t.Iterator[Node]:
        yield self

        for d in self.downstream:
            yield from d.walk()


def lineage(
    column: str | exp.Column,
    sql: str | exp.Expression,
    schema: t.Optional[t.Dict | Schema] = None,
    sources: t.Optional[t.Mapping[str, str | exp.Query]] = None,
    dialect: DialectType = None,
    scope: t.Optional[Scope] = None,
    trim_selects: bool = True,
    **kwargs,
) -> Node:
    """Build the lineage graph for a column of a SQL query.

    This is taken from the `sqlglot.lineage` module and extended with custom features.

    Args:
        column: The column to build the lineage for.
        sql: The SQL string or expression.
        schema: The schema of tables.
        sources: A mapping of queries which will be used to continue building sqlleaf.
        dialect: The dialect of input SQL.
        scope: A pre-created scope to use instead.
        trim_selects: Whether or not to clean up selects by trimming to only relevant columns.
        **kwargs: Qualification optimizer kwargs.

    Returns:
        A lineage node.
    """

    expression = maybe_parse(sql, dialect=dialect)
    column = normalize_identifiers.normalize_identifiers(column, dialect=dialect)

    if sources:
        expression = exp.expand(
            expression,
            {k: t.cast(exp.Query, maybe_parse(v, dialect=dialect)) for k, v in sources.items()},
            dialect=dialect,
        )

    if not scope:
        expression = qualify.qualify(
            expression,
            dialect=dialect,
            schema=schema,
            **{"validate_qualify_columns": False, "identify": False, **kwargs},  # type: ignore
        )

        scope = build_scope(expression)

    if not scope:
        raise SqlglotError("Cannot build lineage, sql must be SELECT")

    if not any(select.alias_or_name == column.name for select in scope.expression.selects):
        raise SqlglotError(f"Cannot find column '{column.name}' in query.")

    return to_node(column, scope, dialect, trim_selects=trim_selects)


def is_expr_in_case_when_statement(expr: exp.Expression) -> bool:
    """
    Determine if the expression is the `x` in a `CASE WHEN x ...` statement.
    We iterate upwards in the AST by looking for an `exp.If` inside a `exp.Case` and inspect its args['ifs'].
    We keep `y` in `THEN y`.
    """
    if_expr = expr.find_ancestor(exp.If)
    if if_expr and if_expr.args['true'] != expr and isinstance(if_expr.parent, exp.Case):
        return True
    return False


def to_node(
    column: exp.Column,
    scope: Scope,
    dialect: DialectType,
    scope_name: t.Optional[str] = None,
    upstream: t.Optional[Node] = None,
    source_name: t.Optional[str] = None,
    reference_node_name: t.Optional[str] = None,
    trim_selects: bool = True,
) -> Node:
    # Find the specific select clause that is the source of the column we want.
    # This can either be a specific, named select or a generic `*` clause.
    select = (
        scope.expression.selects[column]
        if isinstance(column, int)
        else next(
            (select for select in scope.expression.selects if select.alias_or_name == column.name),
            exp.Star() if scope.expression.is_star else scope.expression,
        )
    )

    if isinstance(scope.expression, exp.Subquery):
        for source in scope.subquery_scopes:
            return to_node(
                column,
                scope=source,
                dialect=dialect,
                upstream=upstream,
                source_name=source_name,
                reference_node_name=reference_node_name,
                trim_selects=trim_selects,
            )
    if isinstance(scope.expression, exp.SetOperation):
        name = type(scope.expression).__name__.upper()
        upstream = upstream or Node(name=name, column=column, source=scope.expression, expression=select)

        index = (
            column
            if isinstance(column, int)
            else next(
                (
                    i
                    for i, select in enumerate(scope.expression.selects)
                    if select.alias_or_name == column.name or select.is_star
                ),
                -1,  # mypy will not allow a None here, but a negative index should never be returned
            )
        )

        if index == -1:
            raise ValueError(f"Could not find {column.name} in {scope.expression}")

        for s in scope.union_scopes:
            to_node(
                index,
                scope=s,
                dialect=dialect,
                upstream=upstream,
                source_name=source_name,
                reference_node_name=reference_node_name,
                trim_selects=trim_selects,
            )

        return upstream

    if trim_selects and isinstance(scope.expression, exp.Select):
        # For better ergonomics in our node labels, replace the full select with
        # a version that has only the column we care about.
        #   "x", SELECT x, y FROM foo
        #     => "x", SELECT x FROM foo
        source = t.cast(exp.Expression, scope.expression.select(select, append=False))
    else:
        source = scope.expression

    # Create the node for this step in the lineage chain, and attach it to the previous one.
    node = Node(
        name=f"{scope_name}.{column.name}" if scope_name else str(column),
        column=column,
        source=source,
        expression=select,
        source_name=source_name or "",
        reference_node_name=reference_node_name or "",
    )
    logger.debug('Created Node: %s', node.name)

    if upstream:
        upstream.downstream.append(node)
        node.upstream.append(upstream)

    subquery_scopes = {
        id(subquery_scope.expression): subquery_scope for subquery_scope in scope.subquery_scopes
    }

    for subquery in find_all_in_scope(select, exp.UNWRAPPED_QUERIES):
        subquery_scope = subquery_scopes.get(id(subquery))
        if not subquery_scope:
            logger.warning("Unknown subquery scope: %s", subquery.sql(dialect=dialect))
            continue

        for name in subquery.named_selects:
            to_node(
                exp.column(name),
                scope=subquery_scope,
                dialect=dialect,
                upstream=node,
                trim_selects=trim_selects,
            )

    # if the select is a star add all scope sources as downstreams
    if select.is_star:
        for source in scope.sources.values():
            if isinstance(source, Scope):
                source = source.expression
            n = Node(name=_to_node_name(select), column=column, source=source, upstream=[node], expression=source)
            node.downstream.append(n)
            logger.debug('Created Node: %s', node.name)

    # Find all columns that went into creating this one to list their lineage nodes.
    source_columns = util.unique(find_all_in_scope(select, exp.Column))
    #source_columns = sorted(source_columns, key=lambda x: str(x))   # set() returns nondeterministic ordering
    skip = (exp.Identifier, exp.DataType, exp.Var)
    source_exprs = []
    for col in util.unique(walk_in_scope(select)):
        if col.is_leaf() and not isinstance(col, skip) and not is_expr_in_case_when_statement(col):
            source_exprs.append(col)

    #source_exprs = sorted(source_exprs, key=lambda x: str(x))   # set() returns nondeterministic ordering
    logger.debug('Found leaves: %s', [str(s) for s in source_exprs])

    for c in source_exprs:
        n = Node(name=_to_node_name(c), column=c, source=None, upstream=[node], expression=c)
        node.downstream.append(n)
        logger.debug('Created Node: %s', node.name)

    # If the source is a UDTF find columns used in the UTDF to generate the table
    if isinstance(source, exp.UDTF):
        source_columns |= set(source.find_all(exp.Column))
        derived_tables = [
            source.expression.parent
            for source in scope.sources.values()
            if isinstance(source, Scope) and source.is_derived_table
        ]
    else:
        derived_tables = scope.derived_tables

    source_names = {
        dt.alias: dt.comments[0].split()[1]
        for dt in derived_tables
        if dt.comments and dt.comments[0].startswith("source: ")
    }

    pivots = scope.pivots
    pivot = pivots[0] if len(pivots) == 1 and not pivots[0].unpivot else None
    if pivot:
        # For each aggregation function, the pivot creates a new column for each field in category
        # combined with the aggfunc. So the columns parsed have this order: cat_a_value_sum, cat_a,
        # b_value_sum, b. Because of this step wise manner the aggfunc 'sum(value) as value_sum'
        # belongs to the column indices 0, 2, and the aggfunc 'max(price)' without an alias belongs
        # to the column indices 1, 3. Here, only the columns used in the aggregations are of interest
        # in the lineage, so lookup the pivot column name by index and map that with the columns used
        # in the aggregation.
        #
        # Example: PIVOT (SUM(value) AS value_sum, MAX(price)) FOR category IN ('a' AS cat_a, 'b')
        pivot_columns = pivot.args["columns"]
        pivot_aggs_count = len(pivot.expressions)

        pivot_column_mapping = {}
        for i, agg in enumerate(pivot.expressions):
            agg_cols = list(agg.find_all(exp.Column))
            for col_index in range(i, len(pivot_columns), pivot_aggs_count):
                pivot_column_mapping[pivot_columns[col_index].name] = agg_cols

    for c in source_columns:
        table = c.table
        source = scope.sources.get(table)

        if isinstance(source, Scope):
            reference_node_name = None
            if source.scope_type == ScopeType.DERIVED_TABLE and table not in source_names:
                reference_node_name = table
            elif source.scope_type == ScopeType.CTE:
                selected_node, _ = scope.selected_sources.get(table, (None, None))
                reference_node_name = selected_node.name if selected_node else None

            # The table itself came from a more specific scope. Recurse into that one using the unaliased column name.
            to_node(
                c,
                scope=source,
                dialect=dialect,
                scope_name=table,
                upstream=node,
                source_name=source_names.get(table) or source_name,
                reference_node_name=reference_node_name,
                trim_selects=trim_selects,
            )
        elif pivot and pivot.alias_or_name == c.table:
            downstream_columns = []

            column_name = c.name
            if any(column_name == pivot_column.name for pivot_column in pivot_columns):
                downstream_columns.extend(pivot_column_mapping[column_name])
            else:
                # The column is not in the pivot, so it must be an implicit column of the
                # pivoted source -- adapt column to be from the implicit pivoted source.
                downstream_columns.append(exp.column(c.this, table=pivot.parent.alias_or_name))

            for downstream_column in downstream_columns:
                table = downstream_column.table
                source = scope.sources.get(table)

                if isinstance(source, Scope):
                    to_node(
                        downstream_column,
                        scope=source,
                        scope_name=table,
                        dialect=dialect,
                        upstream=node,
                        source_name=source_names.get(table) or source_name,
                        reference_node_name=reference_node_name,
                        trim_selects=trim_selects,
                    )
                else:
                    source = source or exp.Placeholder()
                    n = Node(
                        name=_to_node_name(downstream_column),
                        # name=downstream_column.sql(comments=False),
                        column=downstream_column,
                        source=source,
                        upstream=[node],
                        expression=source,
                    )
                    node.downstream.append(n)
                    logger.debug('Created Node: %s', node.name)

        else:
            # The source is not a scope and the column is not in any pivot - we've reached the end
            # of the line. At this point, if a source is not found it means this column's lineage
            # is unknown. This can happen if the definition of a source used in a query is not
            # passed into the `sources` map.
            source = source or exp.Placeholder()

            # Change the column's source table to be its name, not its alias
            if isinstance(source, exp.Table):
                if source.catalog:
                    c.set('catalog', exp.to_identifier(source.catalog))
                if source.db:
                    c.set('db', exp.to_identifier(source.db))
                if source.name:
                    c.set('table', exp.to_identifier(source.name))

            n = Node(name=c.sql(comments=False), column=c, source=source, upstream=[node], expression=source)
            node.downstream.append(n)
            logger.debug('Created Node: %s', node.name)

    return node

def _to_node_name(expr):
    return expr.key


def _get_parent_leaves_from_nodes(path, metadata, mapping) -> t.List[structs.GenericNode]:
    """
    Extract the leaves from the lineage Nodes. A leaf is a non-column source of data.
    For example, given
    """
    root_node = path[-1]

    # The `sqlglot` expression chain finishes at Table. We need to finish it at Column.
    expr = root_node.expression
    parent_leaves = to_generic_node(expr, root_node, metadata, mapping)
    return parent_leaves


def _set_outer_functions_for_node(node: Node, prev_arg: t.Union[exp.Expression, structs.OuterFunction], total_function_depth: int):
    """
    Set the outer functions for a Node.

    For example, given "SELECT LOWER(apple) FROM fruit.raw", we set the outer function `LOWER` to be
    associated with the Node `apple`.

    Parameters:
        node (Node): the Node to set outer functions for
        prev_arg: the previous inner argument (e.g. given `LOWER(UPPER(apple))`, prev of `apple` is None, prev of `UPPER` is `apple`)
        total_function_depth: the total number of functions we've seen so far

    Returns:
        prev_value: see description above
    """
    skip = (exp.If, exp.Alias, exp.Paren, exp.EQ, exp.CurrentTimestamp, exp.Column)
    column = node.column
    expr = column
    prev_expr = None

    while True:
        process = True

        if isinstance(expr, int):
            # A union node. This is created by sqlglot.lineage.lineage() and has no effect here.
            return prev_arg

        if isinstance(expr, skip):
            logger.debug('Skipped: %s', type(expr))
            process = False

        elif isinstance(expr, (exp.Anonymous,)) and isinstance(expr.parent, (exp.Dot,)):
            """
            Extract the user-defined function from the 'left' & 'right' properties.
            e.g. etl.my_function('a', 'b')
                -> left = etl
                -> right = my_function
                -> args = ['a', 'b']
            """
            expr_child = expr
            expr = expr.parent

            schema = str(expr.left.name)
            function = str(expr.right.name)

            function_name = f'{schema}.{function}'
            function_kind = 'function'
            function_args = expr_child.args['expressions']

        elif isinstance(expr, (exp.Func, exp.Binary)):
            """
            Extract a system function, e.g. LOWER(), SUBSTRING(), CONCAT()
            """
            function_name = expr.key
            function_kind = 'function'
            function_args = list(expr.args.values())
            function_args = util.flatten(function_args)
            function_args = [arg for arg in function_args if arg and type(arg) != exp.If]

        else:
            logger.debug('Ignoring traversal of %s', type(expr))
            process = False

        if process:
            outer_function = structs.OuterFunction(kind=function_kind, name=function_name, depth=total_function_depth)
            outer_function.add_arguments(function_args, column, prev_arg, prev_expr)

            logger.debug('Outer function: %s', outer_function.name)
            node.outer_functions.append(outer_function)
            prev_arg = outer_function

            logger.debug('Added: %s, %s', type(expr), str(expr.sql(comments=False)))
            total_function_depth += 1

        if expr.parent == expr.parent_select:
            break

        prev_expr = expr
        expr = expr.parent

    return prev_arg


def _convert_lineage_nodes_to_leaves(path: t.List[Node]) -> t.List:
    """
    Convert sqlglot's lineage chain of Node to a custom type that contains only the information we need.
    Associate each Node with its outer functions.

    Parameters:
        path: a list of lineage Nodes
    """
    all_outer_functions = []
    total_function_depth = 0

    root_node = path[-1]
    prev_value = root_node.column

    for node in reversed(path):
        """
        Iterate over the lineage Nodes in reverse (i.e. bottom-up) since there may be multiple downstream paths for each Node, giving them different function depths
        depending on the path they're currently on.
        """
        prev_value = _set_outer_functions_for_node(node, prev_value, total_function_depth)
        all_outer_functions.extend(node.outer_functions)
        total_function_depth += len(node.outer_functions)

    return all_outer_functions


def _is_view(obj: exp.Table):
    return obj.key == 'view'


def _get_all_paths_from_lineage(node: Node, path=[]):
    """
    Iterate over the tree of lineage.Node produced by lineage.lineage()
    """
    path.append(node)
    expr = _unwrap_expression(node.expression)

    if isinstance(expr, exp.Window):
        # Short circuit window functions so that false positive lineage isn't included
        yield path

    elif not node.downstream:
        yield path

    else:
        for child in node.downstream:
            yield from _get_all_paths_from_lineage(child, path)

    path.pop()


def get_lineage_for_columns(child_columns, child_table, statement, dialect, mapping, metadata, statement_index) -> nx.MultiDiGraph:
    statement_graph = nx.MultiDiGraph()
    scope = sqlglot.optimizer.build_scope(statement)

    for select_idx, (col_name, col_type) in enumerate(child_columns.items()):
        logger.info('Calculating lineage. Column: %s, Table: %s, Index: %s', col_name, child_table.name, select_idx)
        col_expr = exp.column(db=child_table.db, table=child_table.name, col=col_name)
        child_column = structs.GenericNode(kind='column', schema=child_table.db, table=child_table.name, column=col_name, column_type=exp.DataType.build(col_type), is_view=_is_view(child_table), expr=col_expr)

        # trim_selects=false -> 10x faster, skips re-parsing
        lin = lineage(column=col_name, sql=statement, scope=scope, dialect=dialect, schema=mapping,trim_selects=False)

        """
        Find and set the outer functions for each Node, and then convert the Node's Expression into
        an common object type that contains only the essential information.
        """
        for path_idx, path in enumerate(_get_all_paths_from_lineage(lin)):
            msg = f'Processing lineage path:', [node.name for node in path], f'Statement: {statement_index}, Select Idx: {select_idx}, Path Idx: {path_idx}'
            logger.debug(msg)
            all_outer_functions = _convert_lineage_nodes_to_leaves(path)
            parent_leaves = _get_parent_leaves_from_nodes(path, metadata, mapping)

            for parent_leaf in parent_leaves:
                edge_attributes = structs.EdgeAttributes(parent=parent_leaf, child=child_column, functions=all_outer_functions, metadata=metadata, statement_idx=statement_index, select_idx=select_idx, path_idx=path_idx)
                statement_graph.add_edge(parent_leaf.name, child_column.name, attrs=edge_attributes)

    return statement_graph


def _get_node_leaves(expr: exp.Expression):
    excl_types = (exp.DataType, exp.Var, exp.Table, exp.Column, exp.Identifier)
    leaves = [l for l in expr.walk() if l.is_leaf() and not isinstance(l, excl_types)]
    return leaves


def _unwrap_expression(expr: exp.Expression) -> exp.Expression:
    """
    Extract the expression from underneath an Alias or a Paren.
    """
    ex = expr
    while isinstance(ex, (exp.Alias, exp.Paren)):
        ex = ex.unalias().unnest()
    return ex


def to_generic_node(an_expr: exp.Expression, node: Node, metadata: structs.DDLMetadata, mapping: sqlglot.MappingSchema) -> t.List:
    """
    Collect the leaves of an expression so that we can get the full set of data sources and function arguments
    for a particular column.
    """
    # An expression may expand out to have multiple parents.
    parent_objects = []

    # Get the type from the parent if unavailable.
    # Literals can have their types in their parent Aliases
    if (not an_expr.type or an_expr.type == exp.DataType.Type.UNKNOWN) and an_expr.parent:
        expr_type = an_expr.parent.type
    else:
        expr_type = an_expr.type

    expr = _unwrap_expression(an_expr)

    if isinstance(expr, exp.Placeholder):
        try:
            col_type = [arg['type'] for arg in metadata.args if metadata and arg['name'] == node.name][0]
        except IndexError as e:
            col_type = exp.DataType.Type.UNKNOWN
        parent_objects.append(structs.GenericNode(kind='variable', column=node.name, column_type=col_type, expr=expr))

    elif isinstance(expr, exp.Window):
        expr = expr.this
        if isinstance(expr, exp.Anonymous):
            # rank()
            parent_objects.append(structs.GenericNode(kind='window', column=expr.alias_or_name, column_type=exp.DataType.build('INT'), expr=expr))
        else:
            # row_number(), etc
            if expr.key == 'rownumber':
                col_type = exp.DataType.build('INT')
            else:
                col_type = expr_type

            parent_objects.append(structs.GenericNode(kind='window', column=expr.key, column_type=col_type, expr=expr))

    elif isinstance(expr, (exp.Table,)):
        # TODO: test if using the column works instead
        col: exp.Column = node.column
        parent_objects.append(structs.GenericNode(kind='column', schema=col.db, table=col.table, column=col.name, column_type=col.type,expr=col))

    elif isinstance(expr, exp.Literal):
        # e.g. select 'hello' as greeting
        if isinstance(expr.parent, exp.Neg):
            val = '-' + expr.sql(comments=False)
        else:
            val = expr.sql(comments=False)
        parent_objects.append(structs.GenericNode(kind='literal', column=val, column_type=expr_type, expr=expr))

    elif isinstance(expr, exp.AggFunc):
        # e.g. select count(*) as cnt
        parent_objects.extend(to_generic_node(expr.this, node, metadata, mapping))

    elif expr.is_star:
        # e.g. select count(*) as cnt
        # This is called *after* the above IF condition for exp.AggFunc
        parent_objects.append(structs.GenericNode(kind='star', column='*', column_type=exp.DataType.build('UNKNOWN'), expr=expr))

    elif isinstance(expr, exp.Null):
        # e.g. SELECT NULL
        parent_objects.append(structs.GenericNode(kind='literal', column=str(expr.key), column_type=expr_type, expr=expr))

    elif isinstance(expr, exp.Cast):
        # e.g. SELECT col1::timestamp AS col1_time

        ## Skip casts for now since they're troublesome with UDFs
        parent_objects.extend(to_generic_node(expr.this, node, metadata, mapping))

    elif isinstance(expr, (exp.StrToDate, exp.StrToTime)):
        parent_objects.append(structs.GenericNode(kind='literal', column=expr.name, column_type=expr_type, expr=expr))

    elif isinstance(expr, exp.Neg):
        # e.g. SELECT -10
        parent_objects.append(structs.GenericNode(kind='literal', column='-' + expr.name, column_type=expr_type, expr=expr))

    elif isinstance(expr, exp.DPipe):
        # e.g. SELECT 'a' || 'b'
        for op in expr.flatten():
            parent_objects.extend(to_generic_node(op, node, metadata, mapping))

    elif isinstance(expr, exp.Dot):
        # Custom function call
        # e.g. etl.udf_create_surrogate_key()

        for function_arg in expr.right.iter_expressions():
            parents = to_generic_node(function_arg, node, metadata, mapping)
            parent_objects.extend(parents)

    elif isinstance(expr, exp.Case):
        # These occur only when THEN leaves that aren't Columns
        # e.g. SELECT CASE WHEN count(*) > 1 THEN 1 ELSE 0 END AS my_var
        # TODO: this may never occur now that the WHEN segments are 1=1?
        leaves = _get_node_leaves(expr)
        for leaf in leaves:
            parent_objects.extend(to_generic_node(leaf, node, metadata, mapping))

    elif isinstance(expr, exp.Func) and expr.arg_types['this'] == False:
        # Current timestamp, current user, current date, etc, that take no args
        parent_objects.append(structs.GenericNode(kind='function', column=expr.key, column_type=expr_type, expr=expr))

    elif isinstance(expr, exp.Binary):
        # e.g. SELECT 1 + 2 AS age
        left_objects = to_generic_node(expr.left, node, metadata, mapping)
        right_objects = to_generic_node(expr.right, node, metadata, mapping)
        parent_objects.extend(left_objects + right_objects)

    elif isinstance(expr, exp.TimeUnit):
        leaves = _get_node_leaves(expr)
        for leaf in leaves:
            parent_objects.extend(to_generic_node(leaf, node, metadata, mapping))

    elif isinstance(expr, exp.Func):
        parent_objects = to_generic_node(expr.this, node, metadata, mapping)

    elif isinstance(expr, exp.Identifier):
        pass

    elif isinstance(expr, exp.Column):
        parent_objects.append(
            structs.GenericNode(kind='column', schema=expr.db, table=expr.table, column=expr.name, column_type=expr_type, expr=expr))

    else:
        raise ValueError(f'Unknown expression type: {type(expr)}')

    return parent_objects
