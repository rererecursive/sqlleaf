import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from sqlleaf.test_cases import fixtures

DIALECT = 'redshift'

def test_one_function():
    from sqlleaf.test_cases.cases.multi_statements import one as func
    edges = fixtures._build_lineage_from_procedure(func)
    assert edges == func.edges

def test_duplicate_loads():
    from sqlleaf.test_cases.cases.multi_statements import duplicate_loads as func
    edges = fixtures._build_lineage_from_procedure(func)
    assert edges == func.edges
