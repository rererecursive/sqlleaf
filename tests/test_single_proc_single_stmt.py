import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from tests import fixtures

DIALECT = 'redshift'
#
def test_one_function():
    from tests.cases.single_statements import one_function as func
    edges = fixtures._build_lineage_from_procedure(func)
    assert edges == func.edges

def test_two_functions():
    from tests.cases.single_statements import two_functions as func
    edges = fixtures._build_lineage_from_procedure(func)
    assert edges == func.edges

def test_multi_function():
    from tests.cases.single_statements import multi_function as func
    edges = fixtures._build_lineage_from_procedure(func)
    assert edges == func.edges

def test_user_function():
    from tests.cases.single_statements import user_function as func
    edges = fixtures._build_lineage_from_procedure(func)
    assert edges == func.edges

def test_procedure_variable():
    from tests.cases.single_statements import procedure_variable as func
    edges = fixtures._build_lineage_from_procedure(func)
    assert edges == func.edges

def test_simple_case_statement():
    from tests.cases.single_statements import simple_case_statement as func
    edges = fixtures._build_lineage_from_procedure(func)
    assert edges == func.edges

def test_complex_case_statement():
    from tests.cases.single_statements import complex_case_statement as func
    edges = fixtures._build_lineage_from_procedure(func)
    assert edges == func.edges

def test_str_concat():
    from tests.cases.single_statements import str_concat as func
    edges = fixtures._build_lineage_from_procedure(func)
    assert edges == func.edges

def test_count_star():
    from tests.cases.single_statements import count_star as func
    edges = fixtures._build_lineage_from_procedure(func)
    assert edges == func.edges

def test_system_function():
    from tests.cases.single_statements import system_function as func
    edges = fixtures._build_lineage_from_procedure(func)
    assert edges == func.edges

def test_integers():
    from tests.cases.single_statements import integers as func
    edges = fixtures._build_lineage_from_procedure(func)
    assert edges == func.edges

def test_window_functions():
    from tests.cases.single_statements import window_functions as func
    edges = fixtures._build_lineage_from_procedure(func)
    assert edges == func.edges

def test_table_alias():
    from tests.cases.single_statements import table_alias as func
    edges = fixtures._build_lineage_from_procedure(func)
    assert edges == func.edges

def test_union():
    from tests.cases.single_statements import union as func
    edges = fixtures._build_lineage_from_procedure(func)
    assert edges == func.edges

def test_subquery_and_with():
    from tests.cases.single_statements import subquery_and_with as func
    edges = fixtures._build_lineage_from_procedure(func)
    assert edges == func.edges

def test_rename_select_aliases():
    from tests.cases.single_statements import rename_select_aliases as func
    edges = fixtures._build_lineage_from_procedure(func)
    assert edges == func.edges

def test_variety_of_types():
    from tests.cases.single_statements import variety_of_types as func
    edges = fixtures._build_lineage_from_procedure(func)
    assert edges == func.edges
