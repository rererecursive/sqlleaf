query = """
INSERT INTO fruit.processed
SELECT
    row_number() over (order by true) as num,
    rank() over (partition by age order by created_at) as rnk
FROM fruit.raw
"""

schema = {
    'fruit': {
        'raw': {
            'age': 'INT',
            'created_at': 'TIMESTAMP',
        },
        'processed': {
            'num': 'INT',
            'rnk': 'INT',
        }
    }
}

edges = [
{
        'parent': {
            'schema': '',
            'table': '',
            'column': 'rownumber',
            'column_type': 'INT',
            'kind': 'window',
            'is_view': False,

        },
        'child': {
            'schema': 'fruit',
            'table': 'processed',
            'column': 'num',
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
            'column': 'rank',
            'column_type': 'INT',
            'kind': 'window',
            'is_view': False,

        },
        'child': {
            'schema': 'fruit',
            'table': 'processed',
            'column': 'rnk',
            'column_type': 'INT',
            'kind': 'column',
            'is_view': False,

        },
        'functions': []
    },
]
