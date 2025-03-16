query = """
INSERT INTO fruit.processed
SELECT
    lower(apple) as apple
FROM fruit.raw
UNION
SELECT
    upper(apple) as apple
FROM fruit.new
"""

schema = {
    'fruit': {
        'raw': {
            'apple': 'VARCHAR',
        },
        'new': {
            'apple': 'VARCHAR',
        },
        'processed': {
            'apple': 'VARCHAR',
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
      }
    ]
  },
  {
    "parent": {
      "schema": "fruit",
      "table": "new",
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
        "name": "upper",
        "kind": "function",
        "depth": 0,
        "arguments": [
          {
            "value": "fruit.new.apple",
            "kind": "column",
            "position": 0,
            "parent_path": True
          }
        ]
      }
    ]
  }
]
