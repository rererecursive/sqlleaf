query = """
INSERT INTO fruit.processed
WITH some_cte AS ( 
    SELECT 
        'red' as apple,
        SUM(amount) as amt
    FROM fruit.destroyed
)
SELECT
    COUNT(r.apple) AS cnt,
    cte.apple as other_apple,
    cte.amt as other_amount
FROM fruit.raw r
INNER JOIN some_cte cte
ON r.apple = cte.apple
"""

schema = {
    'fruit': {
        'destroyed': {
          'amount': 'INT',
        },
        'raw': {
            'apple': 'VARCHAR'
        },
        'processed': {
            'apple': 'VARCHAR',
            'cnt': 'INT',
            'other_apple': 'VARCHAR',
            'other_amount': 'INT'
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
  },
  {
    "parent": {
      "schema": "",
      "table": "",
      "column": "'red'",
      "column_type": "VARCHAR",
      "kind": "literal",
      "is_view": False
    },
    "child": {
      "schema": "fruit",
      "table": "processed",
      "column": "other_apple",
      "column_type": "VARCHAR",
      "kind": "column",
      "is_view": False
    },
    "functions": []
  },
  {
    "parent": {
      "schema": "fruit",
      "table": "destroyed",
      "column": "amount",
      "column_type": "INT",
      "kind": "column",
      "is_view": False
    },
    "child": {
      "schema": "fruit",
      "table": "processed",
      "column": "other_amount",
      "column_type": "INT",
      "kind": "column",
      "is_view": False
    },
    "functions": [
      {
        "name": "sum",
        "kind": "function",
        "depth": 0,
        "arguments": [
          {
            "value": "fruit.destroyed.amount",
            "kind": "column",
            "position": 0,
            "parent_path": True
          }
        ]
      }
    ]
  }
]
