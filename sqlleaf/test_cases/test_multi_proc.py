import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from sqlleaf.test_cases import fixtures

DIALECT = 'redshift'

def test_two_simple_procedures():
    from sqlleaf.test_cases.cases.multi_proc import simple_statements as func
    edges = fixtures._get_lineage_from_multiple_procedures(func)
    assert edges == func.edges

def test_two_complex_procedures():
    from sqlleaf.test_cases.cases.multi_proc import complex_statements as func
    edges = fixtures._get_lineage_from_multiple_procedures(func)
    assert edges == func.edges
