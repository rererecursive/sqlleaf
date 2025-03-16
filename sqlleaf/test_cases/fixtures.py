import sys
import os

import networkx as nx

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

import typing as t
import pytest
import json

import sqlglot
import sqlleaf

DIALECT = 'redshift'


def _build_lineage_from_procedure(func):
    """
    Extract the lineage from an SQL statement into a multidigraph and return its edges.
    """
    mapping = sqlglot.MappingSchema(schema=func.schema, dialect=DIALECT)

    text = _build_query(inner_query=func.query)
    lineage = sqlleaf.get_lineage_from_procedure(text=text, dialect=DIALECT, mapping=mapping, path='')
    edges = lineage.to_json()

    print(json.dumps(edges, indent=2))
    return edges


def _get_lineage_from_multiple_procedures(func):
    """
    Extract the lineage from an SQL statement into a multidigraph and return its edges.
    """
    mapping = sqlglot.MappingSchema(schema=func.schema, dialect=DIALECT)

    lineage = sqlleaf.structs.LineageHolder()

    for query in func.queries:
        text = _build_query(inner_query=query)
        lineage_proc = sqlleaf.get_lineage_from_procedure(text=text, dialect=DIALECT, mapping=mapping, path='')
        lineage.add_graph(lineage_proc.graph)

    edges = lineage.to_json()
    print(json.dumps(edges, indent=2))
    return edges


def _build_query(inner_query: str):
    return '''
CREATE OR REPLACE PROCEDURE fruit.process(v_kind VARCHAR, v_amount INT)
	LANGUAGE plpgsql
	SECURITY DEFINER
AS $$

DECLARE
    v_name VARCHAR;

BEGIN

    %s
    ;

EXCEPTION WHEN OTHERS THEN
	SELECT 1;
END;
$$;

    ''' % (inner_query,)
