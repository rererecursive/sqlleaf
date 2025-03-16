query = """
INSERT INTO fruit.processed
SELECT v_amount as amount
"""

schema = {
    'fruit': {
        'processed': {
            'amount': 'INT',
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
  }
]
