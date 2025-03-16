query = """
INSERT INTO fruit.processed
SELECT
    SUBSTRING(UPPER('ABC'), GREATEST(age), 5) AS apple
FROM fruit.raw
"""

schema = {
    'fruit': {
        'raw': {
            'age': 'INT'
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
      "column": "5",
      "column_type": "INT",
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
    "functions": [
      {
        "name": "substring",
        "kind": "function",
        "depth": 0,
        "arguments": [
          {
            "value": "upper",
            "kind": "function",
            "position": 0,
            "parent_path": False
          },
          {
            "value": "greatest",
            "kind": "function",
            "position": 1,
            "parent_path": False
          },
          {
            "value": "5",
            "kind": "literal",
            "position": 2,
            "parent_path": True
          }
        ]
      }
    ]
  },
  {
    "parent": {
      "schema": "",
      "table": "",
      "column": "'ABC'",
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
    "functions": [
      {
        "name": "upper",
        "kind": "function",
        "depth": 0,
        "arguments": [
          {
            "value": "'ABC'",
            "kind": "literal",
            "position": 0,
            "parent_path": True
          }
        ]
      },
      {
        "name": "substring",
        "kind": "function",
        "depth": 1,
        "arguments": [
          {
            "value": "upper",
            "kind": "function",
            "position": 0,
            "parent_path": True
          },
          {
            "value": "greatest",
            "kind": "function",
            "position": 1,
            "parent_path": False
          },
          {
            "value": "5",
            "kind": "literal",
            "position": 2,
            "parent_path": False
          }
        ]
      }
    ]
  },
  {
    "parent": {
      "schema": "fruit",
      "table": "raw",
      "column": "age",
      "column_type": "INT",
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
        "name": "greatest",
        "kind": "function",
        "depth": 0,
        "arguments": [
          {
            "value": "fruit.raw.age",
            "kind": "column",
            "position": 0,
            "parent_path": True
          }
        ]
      },
      {
        "name": "substring",
        "kind": "function",
        "depth": 1,
        "arguments": [
          {
            "value": "upper",
            "kind": "function",
            "position": 0,
            "parent_path": False
          },
          {
            "value": "greatest",
            "kind": "function",
            "position": 1,
            "parent_path": True
          },
          {
            "value": "5",
            "kind": "literal",
            "position": 2,
            "parent_path": False
          }
        ]
      }
    ]
  }
]