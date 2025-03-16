import hashlib
import logging
import typing as t

import networkx as nx
from sqlglot import exp, MappingSchema

from sqlleaf import util

logger = logging.getLogger('sqleaf')


class LineagePath:
    def __init__(self, root: str, edges: t.List[t.Tuple[str, str, t.Dict]]):
        self.root = root
        self.edges = edges
        self.path_length = len(edges)
        self.path_id = hashlib.md5(':'.join(self.get_edge_identifiers()).encode()).hexdigest()[:16]

    def get_edge_identifiers(self):
        """
        In order to distinguish between multiple edges that are part of the same path,
        we need to create a unique identifier based off data that differentiates them.
        This is done using the edges' "identifier" attribute.
        """
        return [data['attrs'].identifier for par, chi, data in self.edges]


class DDLMetadata:
    def __init__(self, kind: str, stmt: exp.Expression, text: str, file_path: str):
        self.name = str(stmt.this.this)     # e.g. 'etl.my_proc(v_session_id VARCHAR)',
        self.signature = str(stmt.this)     # e.g. 'etl.my_proc'
        self.file_hash = hashlib.md5(text.encode('utf-8')).hexdigest()[0:16]
        self.file_name = file_path

        self.args = [  # e.g. {'name': 'v_session_id', 'type': 'VARCHAR'}
            {'name': str(col.this), 'type': (col.kind if kind == 'PROCEDURE' else exp.DataType.build('UNKNOWN'))}
            for col in stmt.this.find_all(exp.ColumnDef)
        ]

    def to_dict(self):
        return {
            'name': self.name,
            'signature': self.signature,
            'args': self.args,
            'file_hash': self.file_hash,
            'file_name': self.file_name,
        }


class GenericNode:
    def __init__(self, kind: str, column_type: exp.DataType, schema: str = '', table: str = '', column: str = '', is_view: bool = False, expr: exp.Expression = None):
        self.kind = kind
        self.schema = schema
        self.table = table
        self.column = column
        self._column_type = column_type
        self.column_type = str(column_type)
        self.is_view = is_view
        self.expr = expr
        self.type = util.type_name(expr)

        self.set_identifier()

    def set_identifier(self):
        fields = [self.schema, self.table, self.column, self.column_type, str(self.is_view), util.type_name(self.expr)]
        self.id = hashlib.md5(':'.join(fields).encode('utf-8')).hexdigest()[:16]

    def to_dict(self):
        return {
            'schema': self.schema,
            'table': self.table,
            'column': self.column,
            'column_type': self.column_type,
            'kind': self.kind,
            'is_view': self.is_view,
        }

    @property
    def name(self):
        return self.column


class OuterFunction:
    """
    Represents a function extracted by traversing the Expressions inside each GenericNode returned from lineage().
    """
    def __init__(self, kind: str, name: str, depth: int):
        self.kind: str = kind
        self.name: str = name
        self.depth: int = depth   # The position in the function chain (inner = lower)
        self.arguments: t.List[OuterFunctionArgument] = []

    def add_arguments(self, function_arguments: t.List, column, prev_arg, prev_expr):
        pos = 0
        for argument in function_arguments:
            if argument in [True, []]:
                # Ignore empty values inside the 'args' properties, e.g. inside a `count()` expression
                continue

            if argument == column:
                # Root or descendent of root
                arg = OuterFunctionArgument(value=prev_arg, position=pos, parent_path=True)

            elif argument == prev_expr:
                # Reference to an inner function
                arg = OuterFunctionArgument(value=prev_arg, position=pos, parent_path=True)
            else:
                # Reference to another Column or a Literal
                arg = OuterFunctionArgument(value=argument, position=pos, parent_path=False)

            arg.set_parent_outer_function(self)
            self.arguments.append(arg)
            pos += 1


    def to_dict(self):
        return {
            'name': self.name,
            'kind': self.kind,
            'depth': self.depth,
            'arguments': [arg.to_dict() for arg in self.arguments],
        }


class OuterFunctionArgument:
    """
    Represents function arguments inside OuterFunction.
    """
    def __init__(self, value: t.Union[exp.Expression, OuterFunction], position: int, parent_path: bool = False):
        """
        column: the lineage Column | Literal etc
        position: the position of the argument [ e.g. b=1 in func(a,b,c) ]
        parent_path: whether this argument is on the lineage path/chain to the root node
        """
        self.value = value
        self.position = position
        self.parent_path = parent_path
        self.parent_outer_function: OuterFunction = None   # the enclosing OuterFunction

        if isinstance(value, (exp.Func, exp.Timestamp, OuterFunction)):
            self.kind = 'function'
        else:
            self.kind = util.type_name(value)


    def set_parent_outer_function(self, func: OuterFunction):
        """
        Create a link to the enclosing outer function.
        """
        self.parent_outer_function = func

    def to_dict(self):
        return {
            'value': self.name,
            'kind': self.kind,
            'position': self.position,
            'parent_path': self.parent_path,
        }

    @property
    def name(self):
        """
        This is messy, but each of the types has a different attribute containing the value we need.
        """

        if isinstance(self.value, (exp.Star, OuterFunction)):
            return str(self.value.name)
        else:
            if isinstance(self.value, exp.Column):
                return str(self.value)
            elif isinstance(self.value, exp.Literal):
                return self.value.sql(comments=False)
            else:
                return self.value.key


class EdgeAttributes:
    def __init__(self, parent: GenericNode, child: GenericNode, functions: t.List[OuterFunction], metadata: DDLMetadata, statement_idx: int, select_idx: int, path_idx: int):
        self.parent = parent
        self.child = child
        self.functions = functions
        self.metadata = metadata

        # These positions help unique identify syntax inside a set of SQL statements
        self.statement_idx = statement_idx      # The sequence of this statement in a set of statements (e.g. SELECT ...; SELECT ...;)
        self.select_idx = select_idx            # The sequence of this column inside a set of column (e.g. SELECT 'a', 'b', 'c')
        self.path_idx = path_idx                # The sequence of this edge inside a set of identical edges (e.g. two edges between nodes A->B)

        self.create_edge_identifier()
        self.path_ids = []

    def create_edge_identifier(self):
        edge_identifier = ':'.join([str(s) for s in [
            (self.metadata and self.metadata.file_name) or '',
            self.parent.name,
            self.child.name,
            self.statement_idx,
            self.select_idx,
            self.path_idx
        ]])
        self.identifier = hashlib.md5(edge_identifier.encode()).hexdigest()[:16]

    def add_path_id(self, path_id: str):
        self.path_ids.append(path_id)

    def get_attributes(self):
        return {
            'parent': self.parent.name,
            'parent_type': self.parent.column_type,
            'parent_kind': self.parent.kind,
            'child': self.child.name,
            'child_type': self.child.column_type,
            'child_kind': self.child.kind,
            'functions': ','.join(self.get_function_names()),
            'stored_procedure': (self.metadata and self.metadata.file_name) or '',
            'statement_idx': self.statement_idx,
            'select_idx': self.select_idx,
            'path_idx': self.path_idx,
        }

    def get_function_names(self):
        return util.unique([func.name for func in self.functions])


class LineageHolder:
    """
    Holds the lineage in form of a networkx graph and provides functions to extract useful information.
    """
    def __init__(self):
        self.graph = nx.MultiDiGraph()

    def add_graph(self, g: nx.MultiDiGraph):
        self.graph.add_edges_from(g.edges(data=True))

    def iter_edges(self):
        for edge in self.graph.edges.data():
            yield edge

    def to_json(self, full=False) -> t.List[t.Dict]:
        """
        Collect the attributes from each graph edge that are relevant for testing.
        """
        edges = []

        # Sort the graph's edges by the statement index and the path index.
        # This helps us avoid ordering issues when iterating over similar ones.
        for parent, child, data in sorted(self.graph.edges.data(), key=lambda x: (
                x[2]['attrs'].statement_idx,
                x[2]['attrs'].select_idx,
                x[2]['attrs'].path_idx
        )):
            attrs: EdgeAttributes = data['attrs']
            p: GenericNode = attrs.parent
            c: GenericNode = attrs.child
            f: t.List[OuterFunction] = attrs.functions

            if full:
                struct = {
                    'identifier': attrs.identifier,
                    'parent': p.to_dict(),
                    'child': c.to_dict(),
                    'functions': [func.to_dict() for func in f],
                    'metadata': attrs.metadata.to_dict(),
                    'indices': {
                        'statement_idx': attrs.statement_idx,
                        'select_idx': attrs.select_idx,
                        'path_idx': attrs.path_idx,
                    },
                }
            else:
                struct = {
                    'parent': p.to_dict(),
                    'child': c.to_dict(),
                    'functions': [func.to_dict() for func in f],
                }
            edges.append(struct)

        return edges

    def get_edges_without_functions(self) -> nx.MultiDiGraph:
        """
        Get all edges that have no functions.
        If there are two edges from A -> B and one of them has a function, they are both skipped.

        This is useful for determining the foreign keys for a table.
        """
        def filter_no_functions(n1, n2, edge_key):
            attrs: EdgeAttributes = self.graph[n1][n2][edge_key]['attrs']
            return len(attrs.functions) == 0

        return nx.subgraph_view(self.graph, filter_edge=filter_no_functions)


    def get_edges_containing_function(self, name: str, arguments: t.List[t.Dict] = {}) -> nx.MultiDiGraph:
        """
        Get all edges that have a particular function name.
        Optionally narrow the search to those only containing certain arguments.

        Example:
            # Edges with function name `LOWER()`
            get_edges_with_function(mg, name='lower')

            # Edges with function name `ETL.TRANSFORM(fruit.apple)` where the column is in any position
            get_edges_with_function(mg, name='etl.transform', arguments=[{'value':'fruit.apple', 'kind':'column'}])

        The list of column types are: ['column', 'function', 'literal', 'variable']

        Parameters:
            mg:
            name:
            arguments:

        """
        def filter_containing_function(n1, n2, edge_key):
            attrs: EdgeAttributes = self.graph[n1][n2][edge_key]['attrs']

            funcs = [f for f in attrs.functions if f.name == name]

            if not funcs:
                # Mismatch: no function with the name was found
                return False

            if not arguments:
                # Match: a function with the name was found, but no arguments were supplied
                return True

            func = funcs[0]

            # Check if the arguments match.
            if not func.arguments:
                # Mismatch: we supplied arguments but the function has none
                return False

            for arg in arguments:
                for func_arg in func.arguments:
                    if func_arg.kind == arg['kind'] and func_arg.name == arg['value']:
                        # We've found the function; check the position
                        if 'position' not in arg:
                            # Match: we've found the function but its position is not required
                            return True

                        if arg['position'] == func_arg.position:
                            # Match: we've found the function and its positionis correct
                            return True
                        else:
                            # Mismatch: we've found the function but its position is incorrect
                            logging.warning("Found argument %s but in incorrect position (expected=%s, actual=%s)" %
                                (func_arg.value, func_arg.position, arg['position'])
                            )
                            return False
                else:
                    logging.warning("Could not find argument '%s' in function arguments" % (arg,))
                    return False

        return nx.subgraph_view(self.graph, filter_edge=filter_containing_function)
