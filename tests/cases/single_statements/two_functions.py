query = """
INSERT INTO fruit.processed
SELECT
    UPPER(LOWER(apple)) AS apple
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
      "column": "apple",
      "column_type": "VARCHAR",
      "kind": "column",
      "is_view": False
    },
    "functions": [
      {
        "name": "lower",
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
      },
      {
        "name": "upper",
        "kind": "function",
        "depth": 1,
        "arguments": [
          {
            "value": "lower",
            "kind": "function",
            "position": 0,
            "parent_path": True
          }
        ]
      }
    ]
  }
]