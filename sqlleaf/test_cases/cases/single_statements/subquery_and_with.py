query = """
INSERT INTO fruit.processed
WITH subq AS (
    SELECT
      lower(apple) as apple,
      (SELECT count(apple) FROM fruit.old) as cnt
    FROM fruit.new
)
SELECT
    r.food,
    sq.apple,
    sq.cnt
FROM fruit.raw r
INNER JOIN subq sq
ON sq.apple = r.apple
"""

schema = {
    'fruit': {
        'raw': {
            'food': 'VARCHAR',
            'apple': 'VARCHAR',
        },
        'new': {
            'apple': 'VARCHAR',
        },
        'apple': {
            'apple': 'VARCHAR',
        },
        'processed': {
            'food': 'VARCHAR',
            'apple': 'VARCHAR',
            'cnt': 'INT',
        }
    }
}

edges = [
  {
    "parent": {
      "schema": "fruit",
      "table": "raw",
      "column": "food",
      "column_type": "VARCHAR",
      "kind": "column",
      "is_view": False
    },
    "child": {
      "schema": "fruit",
      "table": "processed",
      "column": "food",
      "column_type": "VARCHAR",
      "kind": "column",
      "is_view": False
    },
    "functions": []
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
        "name": "lower",
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
  },
  {
    "parent": {
      "schema": "fruit",
      "table": "old",
      "column": "apple",
      "column_type": "UNKNOWN",
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
            "value": "fruit.old.apple",
            "kind": "column",
            "position": 0,
            "parent_path": True
          }
        ]
      }
    ]
  }
]
