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
""",

"""
INSERT INTO fruit.sold
SELECT 
    kind as apple_kind,
    LOWER(apple) as apple_lower
FROM fruit.raw;
"""
]

schema = {
    'fruit': {
        'raw': {
            'apple': 'VARCHAR',
            'kind': 'VARCHAR',
        },
        'processed': {
            'apple': 'VARCHAR',
        },
        'sold': {
            'cnt': 'INT',
            'apple_kind': 'VARCHAR',
            'apple_lower': 'VARCHAR',
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
  },
  {
    "parent": {
      "schema": "fruit",
      "table": "raw",
      "column": "kind",
      "column_type": "VARCHAR",
      "kind": "column",
      "is_view": False
    },
    "child": {
      "schema": "fruit",
      "table": "sold",
      "column": "apple_kind",
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
      "table": "sold",
      "column": "apple_lower",
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
  }
]
