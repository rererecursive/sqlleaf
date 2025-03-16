query = """
INSERT INTO fruit.processed
SELECT
    'hi' || 'yes' as apple
FROM fruit.raw
"""

schema = {
    'fruit': {
        'raw': {
            'apple': 'VARCHAR'
        },
        'processed': {
            'apple': 'VARCHAR',
        }
    }
}

edges = [
  {
    "parent": {
      "schema": "",
      "table": "",
      "column": "'hiyes'",
      "column_type": "VARCHAR",
      "kind": "literal",
      "is_view": False
    },
    "child": {
      "schema": "fruit",
      "table": "processed",
      "column": "apple",
      "column_type": "VARCHAR",
      "kind": "column",
      "is_view": False
    },
    "functions": []
  }
]
