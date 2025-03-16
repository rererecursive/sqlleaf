query = """
INSERT INTO fruit.processed
SELECT
    CASE WHEN 
        LOWER(apple) = 'orange' 
    THEN 
        5 
    ELSE 
        lower(6) 
    END AS is_orange
FROM fruit.raw
"""

schema = {
    'fruit': {
        'raw': {
            'apple': 'VARCHAR'
        },
        'processed': {
            'is_orange': 'INT',
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
      "column": "is_orange",
      "column_type": "INT",
      "kind": "column",
      "is_view": False
    },
    "functions": [
      {
        "name": "case",
        "kind": "function",
        "depth": 0,
        "arguments": [
          {
            "value": "lower",
            "kind": "function",
            "position": 0,
            "parent_path": False
          }
        ]
      }
    ]
  },
  {
    "parent": {
      "schema": "",
      "table": "",
      "column": "6",
      "column_type": "INT",
      "kind": "literal",
      "is_view": False
    },
    "child": {
      "schema": "fruit",
      "table": "processed",
      "column": "is_orange",
      "column_type": "INT",
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
            "value": "6",
            "kind": "literal",
            "position": 0,
            "parent_path": True
          }
        ]
      },
      {
        "name": "case",
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