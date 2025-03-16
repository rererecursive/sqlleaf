query = """
INSERT INTO fruit.processed
SELECT
    SYSDATE as today
"""

schema = {
    'fruit': {
        'processed': {
            'today': 'TIMESTAMP',
        }
    }
}

edges = [
    {
        'parent': {
            'schema': '',
            'table': '',
            'column': 'currenttimestamp',
            'column_type': 'TIMESTAMP',
            'kind': 'function',
            'is_view': False,

        },
        'child': {
            'schema': 'fruit',
            'table': 'processed',
            'column': 'today',
            'column_type': 'TIMESTAMP',
            'kind': 'column',
            'is_view': False,

        },
        'functions': []
    }
]
