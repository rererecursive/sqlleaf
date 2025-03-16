query = """
INSERT INTO fruit.processed
SELECT
    -10 as neg,
    10 as pos
"""

schema = {
    'fruit': {
        'processed': {
            'pos': 'INT',
            'neg': 'INT',
        }
    }
}

edges = [
    {
        'parent': {
            'schema': '',
            'table': '',
            'column': '-10',
            'column_type': 'INT',
            'kind': 'literal',
            'is_view': False,

        },
        'child': {
            'schema': 'fruit',
            'table': 'processed',
            'column': 'neg',
            'column_type': 'INT',
            'kind': 'column',
            'is_view': False,

        },
        'functions': []
    },
{
        'parent': {
            'schema': '',
            'table': '',
            'column': '10',
            'column_type': 'INT',
            'kind': 'literal',
            'is_view': False,

        },
        'child': {
            'schema': 'fruit',
            'table': 'processed',
            'column': 'pos',
            'column_type': 'INT',
            'kind': 'column',
            'is_view': False,

        },
        'functions': []
    }
]
