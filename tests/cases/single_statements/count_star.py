query = """
INSERT INTO fruit.processed
SELECT
    count(*) as apple
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
      "column": "*",
      "column_type": "UNKNOWN",
      "kind": "star",
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
        "name": "count",
        "kind": "function",
        "depth": 0,
        "arguments": [
          {
            "value": "*",
            "kind": "star",
            "position": 0,
            "parent_path": True
          }
        ]
      }
    ]
  }
]