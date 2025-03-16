import csv
import typing as t
import logging

import networkx as nx

from sqlleaf import structs

logger = logging.getLogger('sqlleaf')


def write_to_file(paths: t.List[structs.LineagePath], filename: str):
    logger.info('Writing graph to file: %s', filename)
    rows = []
    for path_id, path in paths.items():
        for i, (par, chi, edge) in enumerate(path.edges):
            attrs: structs.EdgeAttributes = edge['attrs']
            rows.append({
                'path_id': path_id,
                'hop': i+1,
                **attrs.get_attributes()
            })

    with open(filename, 'w') as fh:
        writer = csv.DictWriter(fh, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def read_from_file():
    pass


def add_path_id_and_root_to_edges(mg: nx.MultiGraph):
    """
    Give each path a unique ID according to the edges it constitutes.

    This only makes sense if multiple procedures / multiple graphs need to be merged. This is because the root of a path
    in a graph may change whenever a new graph is merged.

    An edge may belong to multiple paths. This usually indicates a conflict in the ETL processes (e.g. a table's column
    with two sources of INSERTs) but it may still be valid in certain cases, so we permit it.
    """
    all_lineage_paths = {}
    root_columns = [n for n in mg.nodes if mg.in_degree(n) == 0 and mg.out_degree(n) > 0]

    for i, root in enumerate(root_columns):
        for path in find_edge_paths(mg, root):
            if not path:
                continue

            logger.debug('Found path: %s', path)
            lineage_path = structs.LineagePath(root=root, edges=path)
            path_id = lineage_path.path_id

            for par, chi, data in path:
                data['attrs'].add_path_id(path_id)

            all_lineage_paths[path_id] = lineage_path

    return all_lineage_paths


def print_graph(graph: nx.MultiDiGraph):
    print('+' * 40)
    print('Procedure multigraph:', graph)
    for s, d, data in graph.edges.data():
        print('\t', s, '->', d)
        for k, v in data.items():
            if k in ['parent', 'child']:
                print('\t\t', k, '->', v)
            if k == 'functions':
                print('\t\tfunctions ->')
                for func in v:
                    print(func)
                    for arg in func.arguments:
                        print('\t', arg)


def print_paths(graph: nx.MultiDiGraph):
    roots = [x for x in graph.nodes() if graph.out_degree(x) > 0 and graph.in_degree(x) == 0]

    for root in roots:
        for path in find_paths(graph, root):
            print('Path:', path)


def find_edge_paths(g: nx.Graph, start: str, path: t.List=None, seen: t.Set=None):
    """
    Do the same as the regular find_paths(), but iterate over the edges as well.

    Given a graph:
        A -> B -> C -> D

    If there are two edges between A->B and two edges between C->D:
           __         __
          /  \       /  \
     --> A    B --> C    D -->
          \__/       \__/

    then traversing all the edges gives:
        A -> B -> C -> D
        A -> B -> C -> D
        A -> B -> C -> D
        A -> B -> C -> D

    Returns:
        [(A, B, edge_data={x}), (A, B, edge_data={y}) ...]
    """
    if path is None:
        path = []
    if seen is None:
        seen = {start}

    # Get direct descendants
    desc = nx.descendants_at_distance(g, start, 1)
    if not desc:
        yield path
    else:
        for n in desc:
            if n in seen:
                yield path
            else:
                edges = g.get_edge_data(start, n)
                for idx, data in edges.items():
                    hop = (start, n, data)
                    yield from find_edge_paths(g, n, path + [hop], seen.union([n]))


def find_paths(g: nx.Graph, start=0, path: t.List=None, seen: t.Set=None):
    """
    Traverse the descendants of a node.
    """
    if path is None:
        path = [start]
    if seen is None:
        seen = {start}

    # Get direct descendants
    desc = nx.descendants_at_distance(g, start, 1)
    if not desc:
        yield path
    else:
        for n in desc:
            if n in seen:
                yield path
            else:
                yield from find_paths(g, n, path + [n], seen.union([n]))
