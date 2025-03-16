query = """
INSERT INTO fruit.processed
    WITH cte AS ( 
        SELECT upper(kind) AS knd
        FROM fruit.raw
    )
SELECT 
    v_amount as amount,
    1 as number,
    lower(c.knd) as kind
FROM cte c;
"""

schema = {
    'fruit': {
        'processed': {
            'amount': 'INT',
            'number': 'INT',
            'kind': 'VARCHAR',
        },
        'raw': {
            'kind': 'VARCHAR',
        }
    }
}

edges = [
  {
    "parent": {
      "schema": "",
      "table": "",
      "column": "v_amount",
      "column_type": "INT",
      "kind": "variable",
      "is_view": False
    },
    "child": {
      "schema": "fruit",
      "table": "processed",
      "column": "amount",
      "column_type": "INT",
      "kind": "column",
      "is_view": False
    },
    "functions": []
  },
  {
    "parent": {
      "schema": "",
      "table": "",
      "column": "1",
      "column_type": "INT",
      "kind": "literal",
      "is_view": False
    },
    "child": {
      "schema": "fruit",
      "table": "processed",
      "column": "number",
      "column_type": "INT",
      "kind": "column",
      "is_view": False
    },
    "functions": []
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
      "table": "processed",
      "column": "kind",
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
            "value": "fruit.raw.kind",
            "kind": "column",
            "position": 0,
            "parent_path": True
          }
        ]
      },
      {
        "name": "lower",
        "kind": "function",
        "depth": 1,
        "arguments": [
          {
            "value": "upper",
            "kind": "function",
            "position": 0,
            "parent_path": True
          }
        ]
      }
    ]
  }
]
