query = """
INSERT INTO fruit.processed
SELECT
    admin.create_key(SUBSTRING(UPPER('ABC'), GREATEST(dob), 5), address)  AS key
FROM fruit.raw
"""

schema = {
    'fruit': {
        'raw': {
            'address': 'VARCHAR',
            'dob': 'VARCHAR'
        },
        'processed': {
            'key': 'VARCHAR',
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
      "column": "key",
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
      },
      {
        "name": "admin.create_key",
        "kind": "function",
        "depth": 1,
        "arguments": [
          {
            "value": "substring",
            "kind": "function",
            "position": 0,
            "parent_path": True
          },
          {
            "value": "fruit.raw.address",
            "kind": "column",
            "position": 1,
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
      "column": "'ABC'",
      "column_type": "VARCHAR",
      "kind": "literal",
      "is_view": False
    },
    "child": {
      "schema": "fruit",
      "table": "processed",
      "column": "key",
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
      },
      {
        "name": "admin.create_key",
        "kind": "function",
        "depth": 2,
        "arguments": [
          {
            "value": "substring",
            "kind": "function",
            "position": 0,
            "parent_path": True
          },
          {
            "value": "fruit.raw.address",
            "kind": "column",
            "position": 1,
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
      "column": "address",
      "column_type": "VARCHAR",
      "kind": "column",
      "is_view": False
    },
    "child": {
      "schema": "fruit",
      "table": "processed",
      "column": "key",
      "column_type": "VARCHAR",
      "kind": "column",
      "is_view": False
    },
    "functions": [
      {
        "name": "admin.create_key",
        "kind": "function",
        "depth": 0,
        "arguments": [
          {
            "value": "substring",
            "kind": "function",
            "position": 0,
            "parent_path": False
          },
          {
            "value": "fruit.raw.address",
            "kind": "column",
            "position": 1,
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
      "column": "dob",
      "column_type": "VARCHAR",
      "kind": "column",
      "is_view": False
    },
    "child": {
      "schema": "fruit",
      "table": "processed",
      "column": "key",
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
            "value": "fruit.raw.dob",
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
      },
      {
        "name": "admin.create_key",
        "kind": "function",
        "depth": 2,
        "arguments": [
          {
            "value": "substring",
            "kind": "function",
            "position": 0,
            "parent_path": True
          },
          {
            "value": "fruit.raw.address",
            "kind": "column",
            "position": 1,
            "parent_path": False
          }
        ]
      }
    ]
  }
]
