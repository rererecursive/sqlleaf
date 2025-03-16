query = """
INSERT INTO fruit.processed
WITH raws AS ( 
    SELECT 
        UPPER(apple) as upper_apple,
        LOWER(apple) as lower_apple
    FROM fruit.raw
) 
SELECT
    case when 
        nvl(trim(r.upper_apple),trim(r.lower_apple)) is not null
    then 
        coalesce(etl.udf_create_surrogate_key(nvl(nvl(trim(r.upper_apple),trim(r.lower_apple))::varchar, ''),'CCBMIRN'),'UNKNOWN')
    else 
        'NOT_AVAILABLE' 
    end as apple
FROM raws r
"""

schema = {
    'fruit': {
        'raw': {
            'apple': 'VARCHAR',
        },
        'processed': {
            'apple': 'VARCHAR'
        }
    }
}

edges = [
  {
    "parent": {
      "schema": "",
      "table": "",
      "column": "'NOT_AVAILABLE'",
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
        "name": "case",
        "kind": "function",
        "depth": 0,
        "arguments": [
          {
            "value": "'NOT_AVAILABLE'",
            "kind": "literal",
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
      },
      {
        "name": "trim",
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
      },
      {
        "name": "coalesce",
        "kind": "function",
        "depth": 2,
        "arguments": [
          {
            "value": "trim",
            "kind": "function",
            "position": 0,
            "parent_path": True
          },
          {
            "value": "trim",
            "kind": "function",
            "position": 1,
            "parent_path": False
          }
        ]
      },
      {
        "name": "cast",
        "kind": "function",
        "depth": 3,
        "arguments": [
          {
            "value": "coalesce",
            "kind": "function",
            "position": 0,
            "parent_path": True
          },
          {
            "value": "datatype",
            "kind": "datatype",
            "position": 1,
            "parent_path": False
          }
        ]
      },
      {
        "name": "coalesce",
        "kind": "function",
        "depth": 4,
        "arguments": [
          {
            "value": "cast",
            "kind": "function",
            "position": 0,
            "parent_path": True
          },
          {
            "value": "''",
            "kind": "literal",
            "position": 1,
            "parent_path": False
          }
        ]
      },
      {
        "name": "etl.udf_create_surrogate_key",
        "kind": "function",
        "depth": 5,
        "arguments": [
          {
            "value": "coalesce",
            "kind": "function",
            "position": 0,
            "parent_path": True
          },
          {
            "value": "'CCBMIRN'",
            "kind": "literal",
            "position": 1,
            "parent_path": False
          }
        ]
      },
      {
        "name": "coalesce",
        "kind": "function",
        "depth": 6,
        "arguments": [
          {
            "value": "etl.udf_create_surrogate_key",
            "kind": "function",
            "position": 0,
            "parent_path": True
          },
          {
            "value": "'UNKNOWN'",
            "kind": "literal",
            "position": 1,
            "parent_path": False
          }
        ]
      },
      {
        "name": "case",
        "kind": "function",
        "depth": 7,
        "arguments": [
          {
            "value": "'NOT_AVAILABLE'",
            "kind": "literal",
            "position": 0,
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
            "value": "fruit.raw.apple",
            "kind": "column",
            "position": 0,
            "parent_path": True
          }
        ]
      },
      {
        "name": "trim",
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
      },
      {
        "name": "coalesce",
        "kind": "function",
        "depth": 2,
        "arguments": [
          {
            "value": "trim",
            "kind": "function",
            "position": 0,
            "parent_path": False
          },
          {
            "value": "trim",
            "kind": "function",
            "position": 1,
            "parent_path": True
          }
        ]
      },
      {
        "name": "cast",
        "kind": "function",
        "depth": 3,
        "arguments": [
          {
            "value": "coalesce",
            "kind": "function",
            "position": 0,
            "parent_path": True
          },
          {
            "value": "datatype",
            "kind": "datatype",
            "position": 1,
            "parent_path": False
          }
        ]
      },
      {
        "name": "coalesce",
        "kind": "function",
        "depth": 4,
        "arguments": [
          {
            "value": "cast",
            "kind": "function",
            "position": 0,
            "parent_path": True
          },
          {
            "value": "''",
            "kind": "literal",
            "position": 1,
            "parent_path": False
          }
        ]
      },
      {
        "name": "etl.udf_create_surrogate_key",
        "kind": "function",
        "depth": 5,
        "arguments": [
          {
            "value": "coalesce",
            "kind": "function",
            "position": 0,
            "parent_path": True
          },
          {
            "value": "'CCBMIRN'",
            "kind": "literal",
            "position": 1,
            "parent_path": False
          }
        ]
      },
      {
        "name": "coalesce",
        "kind": "function",
        "depth": 6,
        "arguments": [
          {
            "value": "etl.udf_create_surrogate_key",
            "kind": "function",
            "position": 0,
            "parent_path": True
          },
          {
            "value": "'UNKNOWN'",
            "kind": "literal",
            "position": 1,
            "parent_path": False
          }
        ]
      },
      {
        "name": "case",
        "kind": "function",
        "depth": 7,
        "arguments": [
          {
            "value": "'NOT_AVAILABLE'",
            "kind": "literal",
            "position": 0,
            "parent_path": False
          }
        ]
      }
    ]
  }
]
