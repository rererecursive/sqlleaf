queries = [
"""
INSERT INTO fruit.processed
SELECT
    UPPER(apple) as apple
FROM fruit.raw;
""",

"""
INSERT INTO fruit.sold
SELECT 
    COUNT(apple) as cnt
FROM fruit.processed;
"""
]

schema = {
    'fruit': {
        'raw': {
            'apple': 'VARCHAR',
        },
        'processed': {
            'apple': 'VARCHAR',
        },
        'sold': {
            'cnt': 'INT',
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
        "name": "upper",
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
      "table": "processed",
      "column": "apple",
      "column_type": "VARCHAR",
      "kind": "column",
      "is_view": False
    },
    "child": {
      "schema": "fruit",
      "table": "sold",
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
            "value": "fruit.processed.apple",
            "kind": "column",
            "position": 0,
            "parent_path": True
          }
        ]
      }
    ]
  }
]
