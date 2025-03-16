query = """
INSERT INTO fruit.processed (
    outcome,
    kind,
    quality
)
SELECT
    'sold',
    apple,
    'good' as quality
FROM fruit.raw
"""


schema = {
    'fruit': {
        'raw': {
            'apple': 'VARCHAR'
        },
        'processed': {
            'outcome': 'VARCHAR',
            'kind': 'VARCHAR',
            'quality': 'VARCHAR',
        }
    }
}

edges = [
  {
    "parent": {
      "schema": "",
      "table": "",
      "column": "'sold'",
      "column_type": "VARCHAR",
      "kind": "literal",
      "is_view": False
    },
    "child": {
      "schema": "fruit",
      "table": "processed",
      "column": "outcome",
      "column_type": "VARCHAR",
      "kind": "column",
      "is_view": False
    },
    "functions": []
  },
  {
    "parent": {
      "schema": "fruit",
      "table": "raw",
      "column": "apple",
      "column_type": "VARCHAR",
      "kind": "column",
      "is_view": False
    },
    "child": {
      "schema": "fruit",
      "table": "processed",
      "column": "kind",
      "column_type": "VARCHAR",
      "kind": "column",
      "is_view": False
    },
    "functions": []
  },
  {
    "parent": {
      "schema": "",
      "table": "",
      "column": "'good'",
      "column_type": "VARCHAR",
      "kind": "literal",
      "is_view": False
    },
    "child": {
      "schema": "fruit",
      "table": "processed",
      "column": "quality",
      "column_type": "VARCHAR",
      "kind": "column",
      "is_view": False
    },
    "functions": []
  }
]