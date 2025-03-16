import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from tests import fixtures

DIALECT = 'redshift'

def test_one_function():
    from tests.cases.multi_statements import one as func
    edges = fixtures._build_lineage_from_procedure(func)
    assert edges == func.edges

def test_duplicate_loads():
    from tests.cases.multi_statements import duplicate_loads as func
    edges = fixtures._build_lineage_from_procedure(func)
    assert edges == func.edges
