query = """
INSERT INTO fruit.processed
SELECT
    COUNT(apple) AS cnt
FROM fruit.raw
"""


schema = {
    'fruit': {
        'raw': {
            'apple': 'VARCHAR'
        },
        'processed': {
            'apple': 'VARCHAR',
            'cnt': 'INT'
        }
    }
}

edges = [
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
      "column": "cnt",
      "column_type": "INT",
      "kind": "column",
      "is_view": False
    },
    "functions": [
      {
        "name": "count",
        "kind": "function",
        "depth": 0,
        "arguments": [
          {
            "value": "fruit.raw.apple",
            "kind": "column",
            "position": 0,
            "parent_path": True
          }
        ]
      }
    ]
  }
]
