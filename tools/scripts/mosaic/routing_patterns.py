#!/usr/bin/env python

import argparse, itertools, math, string, sys, yaml

## Template
# 'x' : {'maps' : {i: ()} }

map_buckets_by_query = {
  '4': {'maps': { 'ORDER_COUNT'               : (1, [4]),
                  'ORDER_COUNT_mLINEITEM1'    : (2, [4,4]),
                  'ORDER_COUNT_mORDERS3_E1_1' : (3, [4]) }},

  '3': {'maps': { "QUERY3"                        : (1, [4, 4, 4]),
                  "QUERY3_mLINEITEM1"             : (2, [4, 4, 4]),
                  "QUERY3_mLINEITEM1_mCUSTOMER2"  : (3, [4, 4, 4, 4]),
                  "QUERY3_mORDERS1"               : (4, [4]),
                  "QUERY3_mORDERS3"               : (5, [4]),
                  "QUERY3_mORDERS6"               : (6, [4]),
                  "QUERY3_mCUSTOMER2"             : (7, [4, 4, 4, 4]),
                  "QUERY3_mCUSTOMER4"             : (8, [4, 4, 4, 4]) }},

  '10': {'maps': { "REVENUE"                       : (2, [8, 8, 8, 8, 8, 8, 8]),
                   "REVENUE_mLINEITEM2"            : (3, [8, 8, 8, 8, 8, 8, 8, 8]),
                   "REVENUE_mLINEITEM2_mCUSTOMER1" : (4, [8, 8]),
                   "REVENUE_mORDERS1"              : (5, [8]),
                   "REVENUE_mORDERS4"              : (6, [8, 8, 8, 8, 8, 8, 8]),
                   "REVENUE_mORDERS5"              : (7, [8]),
                   "REVENUE_mCUSTOMER1"            : (8, [8]),
                   "REVENUE_mCUSTOMER2"            : (9, [8, 8]),
                   "REVENUE_mCUSTOMER3"            : (10, [8]) }},

  '11a': {'maps': { "QUERY11A"            : (1, [8]),
                    "QUERY11A_mSUPPLIER1" : (2, [8,8]),
                    "QUERY11A_mPARTSUPP1" : (3, [8]) }},

  '12': {'maps': { "HIGH_LINE_COUNT"            : (1, [8]),
                   "HIGH_LINE_COUNT_mLINEITEM1" : (2, [8]),
                   "HIGH_LINE_COUNT_mLINEITEM8" : (3, [8]),
                   "HIGH_LINE_COUNT_mORDERS1"   : (4, [8, 8]),
                   "HIGH_LINE_COUNT_mORDERS4"   : (5, [8, 8]),
                   "LOW_LINE_COUNT"             : (6, [8]),
                   "LOW_LINE_COUNT_mLINEITEM6"  : (7, [8]) }},

  '17': {'maps': { "AVG_YEARLY_pLINEITEM5"           : (1, [8, 8]),
                   "AVG_YEARLY_mLINEITEM1"           : (1, [8]),
                   "AVG_YEARLY_mLINEITEM2_L1_1_L1_1" : (1, [8]),
                   "AVG_YEARLY_mLINEITEM2_L1_2"      : (1, [8]),
                   "AVG_YEARLY_mLINEITEM5"           : (1, [8]) }},

  '18a': {'maps': { "QUERY18"                       : (1, [8]),
                    "QUERY18_mLINEITEM2"            : (2, [8, 8]),
                    "QUERY18_mLINEITEM2_mCUSTOMER1" : (3, [8, 8]),
                    "QUERY18_mLINEITEM5"            : (4, [8, 8]),
                    "QUERY18_mORDERS2"              : (5, [8]),
                    "QUERY18_mCUSTOMER1"            : (6, [8, 8]),
                    "QUERY18_mCUSTOMER1_L1_1_L1_1"  : (7, [8]),
                    "QUERY18_mCUSTOMER1_L1_2"       : (8, [8]) }},

  '18': {'maps': { "QUERY18"                       : (1, [4, 4, 4, 4, 4]),
                   "QUERY18_mORDERS2"              : (2, [4, 4]),
                   "QUERY18_mCUSTOMER1"            : (3, [4, 4, 4, 4]),
                   "QUERY18_mCUSTOMER1_mLINEITEM1" : (4, [4, 4, 4, 4]),
                   "QUERY18_mLINEITEM1"            : (5, [4, 4, 4, 4, 4]),
                   "QUERY18_mLINEITEM1_mLINEITEM1" : (6, [4, 4, 4, 4, 4]),
                   "QUERY18_mLINEITEM1_E1_1_L1_1"  : (7, [4]) }}
}


## Template
# 'x' : {'stmts'    : {i: {'map_vars': []}},
#        'bindings' : {},
#        'binding_patterns': {}},

stmts_by_query = {
	'4' : {'stmts':
                {4: {'map_vars': [("ORDER_COUNT",               ["O_ORDERPRIORITY"]),
                                  ("ORDER_COUNT_mLINEITEM1",    ["O_ORDERPRIORITY", "LINEITEM_ORDERKEY"]),
                                  ("ORDER_COUNT_mORDERS3_E1_1", ["LINEITEM_ORDERKEY"])]}
                 },

               'bindings' : {'LINEITEM': {"LINEITEM_ORDERKEY", "LINEITEM_PARTKEY", "LINEITEM_SUPPKEY", "LINEITEM_LINENUMBER",
                                  "LINEITEM_QUANTITY", "LINEITEM_EXTENDEDPRICE", "LINEITEM_DISCOUNT", "LINEITEM_TAX",
                                  "LINEITEM_RETURNFLAG", "LINEITEM_LINESTATUS", "LINEITEM_SHIPDATE", "LINEITEM_COMMITDATE",
                                  "LINEITEM_RECEIPTDATE", "LINEITEM_SHIPINSTRUCT", "LINEITEM_SHIPMODE", "LINEITEM_COMMENT"},

                             'ORDERS'  : {"ORDERS_ORDERKEY", "ORDERS_CUSTKEY", "ORDERS_ORDERSTATUS", "ORDERS_TOTALPRICE",
                                              "ORDERS_ORDERDATE", "ORDERS_ORDERPRIORITY", "ORDERS_CLERK", "ORDERS_SHIPPRIORITY", "ORDERS_COMMENT"}
                            },

               'binding_patterns': {4: 'LINEITEM'}},


  '3' : {'stmts':
        {16: {'map_vars': [("QUERY3",            ["LINEITEM_ORDERKEY", "ORDERS_ORDERDATE", "ORDERS_SHIPPRIORITY"]),
                           ("QUERY3_mLINEITEM1", ["LINEITEM_ORDERKEY", "ORDERS_ORDERDATE", "ORDERS_SHIPPRIORITY"])]},

         19: {'map_vars': [("QUERY3_mCUSTOMER2",            ["LINEITEM_ORDERKEY", "ORDERS_ORDERDATE", "ORDERS_SHIPPRIORITY", "QUERY3_mCUSTOMERCUSTOMER_CUSTKEY"]),
                           ("QUERY3_mLINEITEM1_mCUSTOMER2", ["LINEITEM_ORDERKEY", "ORDERS_ORDERDATE", "ORDERS_SHIPPRIORITY", "QUERY3_mCUSTOMERCUSTOMER_CUSTKEY"])]},

         20: {'map_vars': [("QUERY3_mCUSTOMER4",            ["LINEITEM_ORDERKEY", "ORDERS_ORDERDATE", "ORDERS_SHIPPRIORITY", "QUERY3_mCUSTOMERCUSTOMER_CUSTKEY"]),
                           ("QUERY3_mLINEITEM1_mCUSTOMER2", ["LINEITEM_ORDERKEY", "ORDERS_ORDERDATE", "ORDERS_SHIPPRIORITY", "QUERY3_mCUSTOMERCUSTOMER_CUSTKEY"])]},

         0: {'map_vars': [("QUERY3",            ["ORDERS_ORDERKEY", "ORDERS_ORDERDATE", "ORDERS_SHIPPRIORITY"]),
                          ("QUERY3_mCUSTOMER2", ["ORDERS_ORDERKEY", "ORDERS_ORDERDATE", "ORDERS_SHIPPRIORITY", "CUSTOMER_CUSTKEY"]),
                          ("QUERY3_mCUSTOMER4", ["ORDERS_ORDERKEY", "ORDERS_ORDERDATE", "ORDERS_SHIPPRIORITY", "CUSTOMER_CUSTKEY"])]},

         1: {'map_vars': [("QUERY3_mLINEITEM1",            ["QUERY3_mLINEITEMLINEITEM_ORDERKEY", "ORDERS_ORDERDATE", "ORDERS_SHIPPRIORITY"]),
                          ("QUERY3_mLINEITEM1_mCUSTOMER2", ["QUERY3_mLINEITEMLINEITEM_ORDERKEY", "ORDERS_ORDERDATE", "ORDERS_SHIPPRIORITY", "CUSTOMER_CUSTKEY"])]}
        },

        'bindings': {'LINEITEM': {"LINEITEM_ORDERKEY", "LINEITEM_PARTKEY", "LINEITEM_SUPPKEY", "LINEITEM_LINENUMBER",
                                  "LINEITEM_QUANTITY", "LINEITEM_EXTENDEDPRICE", "LINEITEM_DISCOUNT", "LINEITEM_TAX",
                                  "LINEITEM_RETURNFLAG", "LINEITEM_LINESTATUS", "LINEITEM_SHIPDATE", "LINEITEM_COMMITDATE",
                                  "LINEITEM_RECEIPTDATE", "LINEITEM_SHIPINSTRUCT", "LINEITEM_SHIPMODE", "LINEITEM_COMMENT"},

                     'ORDERS'  : {"ORDERS_ORDERKEY", "ORDERS_CUSTKEY", "ORDERS_ORDERSTATUS", "ORDERS_TOTALPRICE",
                                  "ORDERS_ORDERDATE", "ORDERS_ORDERPRIORITY", "ORDERS_CLERK", "ORDERS_SHIPPRIORITY", "ORDERS_COMMENT"},

                     'CUSTOMER': {"CUSTOMER_CUSTKEY", "CUSTOMER_NAME", "CUSTOMER_ADDRESS", "CUSTOMER_NATIONKEY", "CUSTOMER_PHONE",
                                  "CUSTOMER_ACCTBAL", "CUSTOMER_MKTSEGMENT", "CUSTOMER_COMMENT"}
                    },

        'binding_patterns': {16: 'LINEITEM', 19: 'LINEITEM', 20: 'LINEITEM',
                             0: 'CUSTOMER', 1: 'CUSTOMER'}
      },

  '10' : {'stmts': {
          16: {'map_vars':
                [("REVENUE",            ["C_CUSTKEY", "C_NAME", "C_ACCTBAL", "N_NAME", "C_ADDRESS", "C_PHONE", "C_COMMENT"]),
                 ("REVENUE_mLINEITEM2", ["C_CUSTKEY", "C_NAME", "C_ACCTBAL", "N_NAME", "C_ADDRESS", "C_PHONE", "C_COMMENT", "LINEITEM_ORDERKEY"])]},

          19: {'map_vars':
                [("REVENUE_mCUSTOMER1",            ["REVENUE_mCUSTOMERCUSTOMER_CUSTKEY"]),
                 ("REVENUE_mLINEITEM2_mCUSTOMER1", ["REVENUE_mCUSTOMERCUSTOMER_CUSTKEY", "LINEITEM_ORDERKEY"])]},

          20: {'map_vars':
                [("REVENUE_mCUSTOMER3",            ["REVENUE_mCUSTOMERCUSTOMER_CUSTKEY"]),
                 ("REVENUE_mLINEITEM2_mCUSTOMER1", ["REVENUE_mCUSTOMERCUSTOMER_CUSTKEY", "LINEITEM_ORDERKEY"])]},

          6: {'map_vars':
                [("REVENUE",          ["ORDERS_CUSTKEY", "C_NAME", "C_ACCTBAL", "N_NAME", "C_ADDRESS", "C_PHONE", "C_COMMENT"]),
                ("REVENUE_mORDERS4", ["ORDERS_CUSTKEY", "C_NAME", "C_ACCTBAL", "N_NAME", "C_ADDRESS", "C_PHONE", "C_COMMENT"]),
                ("REVENUE_mORDERS1", ["ORDERS_ORDERKEY"]),
                ("REVENUE_mORDERS5", ["ORDERS_ORDERKEY"])]},

          7: {'map_vars':
                [("REVENUE_mLINEITEM2", ["ORDERS_CUSTKEY", "C_NAME", "C_ACCTBAL", "N_NAME", "C_ADDRESS", "C_PHONE", "C_COMMENT", "ORDERS_ORDERKEY"]),
                ("REVENUE_mORDERS4",   ["ORDERS_CUSTKEY", "C_NAME", "C_ACCTBAL", "N_NAME", "C_ADDRESS", "C_PHONE", "C_COMMENT"])]},

          0: {'map_vars':
                [("REVENUE",            ["CUSTOMER_CUSTKEY", "CUSTOMER_NAME", "CUSTOMER_ACCTBAL", "N_NAME", "CUSTOMER_ADDRESS", "CUSTOMER_PHONE", "CUSTOMER_COMMENT"]),
                 ("REVENUE_mCUSTOMER2", ["N_NAME", "CUSTOMER_NATIONKEY"]),
                 ("REVENUE_mCUSTOMER1", ["CUSTOMER_CUSTKEY"]),
                 ("REVENUE_mCUSTOMER3", ["CUSTOMER_CUSTKEY"])]},

          2: {'map_vars':
                [("REVENUE_mORDERS4",   ["CUSTOMER_CUSTKEY", "CUSTOMER_NAME", "CUSTOMER_ACCTBAL", "N_NAME", "CUSTOMER_ADDRESS", "CUSTOMER_PHONE", "CUSTOMER_COMMENT"]),
                 ("REVENUE_mCUSTOMER2", ["N_NAME", "CUSTOMER_NATIONKEY"])]}
        },

        'bindings': {'LINEITEM': {"LINEITEM_ORDERKEY", "LINEITEM_PARTKEY", "LINEITEM_SUPPKEY", "LINEITEM_LINENUMBER",
                                  "LINEITEM_QUANTITY", "LINEITEM_EXTENDEDPRICE", "LINEITEM_DISCOUNT", "LINEITEM_TAX",
                                  "LINEITEM_RETURNFLAG", "LINEITEM_LINESTATUS", "LINEITEM_SHIPDATE", "LINEITEM_COMMITDATE",
                                  "LINEITEM_RECEIPTDATE", "LINEITEM_SHIPINSTRUCT", "LINEITEM_SHIPMODE", "LINEITEM_COMMENT"},

                     'ORDERS'  : {"ORDERS_ORDERKEY", "ORDERS_CUSTKEY", "ORDERS_ORDERSTATUS", "ORDERS_TOTALPRICE",
                                  "ORDERS_ORDERDATE", "ORDERS_ORDERPRIORITY", "ORDERS_CLERK", "ORDERS_SHIPPRIORITY", "ORDERS_COMMENT"},

                     'CUSTOMER': {"CUSTOMER_CUSTKEY", "CUSTOMER_NAME", "CUSTOMER_ADDRESS", "CUSTOMER_NATIONKEY", "CUSTOMER_PHONE",
                                  "CUSTOMER_ACCTBAL", "CUSTOMER_MKTSEGMENT", "CUSTOMER_COMMENT"}
                    },

        'binding_patterns': {
          16: 'LINEITEM', 19: 'LINEITEM', 20: 'LINEITEM',
          6: 'ORDERS', 7: 'ORDERS',
          0: 'CUSTOMER', 2: 'CUSTOMER'
        }},

  '11a' : {'stmts'    : {4: {'map_vars': [("QUERY11A",            ["PS_PARTKEY"]),
                                          ("QUERY11A_mSUPPLIER1", ["PS_PARTKEY", "SUPPLIER_SUPPKEY"])]}},

           'bindings' : {'SUPPLIER': {"SUPPLIER_SUPPKEY", "SUPPLIER_NAME", "SUPPLIER_ADDRESS", "SUPPLIER_NATIONKEY", "SUPPLIER_PHONE", "SUPPLIER_ACCTBAL", "SUPPLIER_COMMENT"},
                         'PARTSUPP': {"PARTSUPP_PARTKEY", "PARTSUPP_SUPPKEY", "PARTSUPP_AVAILQTY", "PARTSUPP_SUPPLYCOST", "PARTSUPP_COMMENT"}},

           'binding_patterns': {4: 'SUPPLIER'}},

  '12' : {'stmts'    : {0: {'map_vars': [("HIGH_LINE_COUNT",          ["L_SHIPMODE"]),
                                         ("HIGH_LINE_COUNT_mORDERS1", ["L_SHIPMODE", "ORDERS_ORDERKEY"]),
                                         ("HIGH_LINE_COUNT_mORDERS4", ["L_SHIPMODE", "ORDERS_ORDERKEY"])]},

                        3: {'map_vars': [("LOW_LINE_COUNT",           ["L_SHIPMODE"]),
                                         ("HIGH_LINE_COUNT_mORDERS1", ["L_SHIPMODE", "ORDERS_ORDERKEY"]),
                                         ("HIGH_LINE_COUNT_mORDERS4", ["L_SHIPMODE", "ORDERS_ORDERKEY"])]}},

          'bindings' : {'LINEITEM': {"LINEITEM_ORDERKEY", "LINEITEM_PARTKEY", "LINEITEM_SUPPKEY", "LINEITEM_LINENUMBER",
                                     "LINEITEM_QUANTITY", "LINEITEM_EXTENDEDPRICE", "LINEITEM_DISCOUNT", "LINEITEM_TAX",
                                     "LINEITEM_RETURNFLAG", "LINEITEM_LINESTATUS", "LINEITEM_SHIPDATE", "LINEITEM_COMMITDATE",
                                     "LINEITEM_RECEIPTDATE", "LINEITEM_SHIPINSTRUCT", "LINEITEM_SHIPMODE", "LINEITEM_COMMENT"},

                       'ORDERS'  : {"ORDERS_ORDERKEY", "ORDERS_CUSTKEY", "ORDERS_ORDERSTATUS", "ORDERS_TOTALPRICE",
                                    "ORDERS_ORDERDATE", "ORDERS_ORDERPRIORITY", "ORDERS_CLERK", "ORDERS_SHIPPRIORITY", "ORDERS_COMMENT"}},

          'binding_patterns': {0: 'ORDERS', 3: 'ORDERS'}},

  '17' : {'stmts'    : {2: {'map_vars': [("AVG_YEARLY_mLINEITEM5", ["PART_PARTKEY", "L_QUANTITY"]),
                                         ("AVG_YEARLY_pLINEITEM5", ["PART_PARTKEY", "L_QUANTITY"])]}},

          'bindings' : {'LINEITEM': {"LINEITEM_ORDERKEY", "LINEITEM_PARTKEY", "LINEITEM_SUPPKEY", "LINEITEM_LINENUMBER",
                                     "LINEITEM_QUANTITY", "LINEITEM_EXTENDEDPRICE", "LINEITEM_DISCOUNT", "LINEITEM_TAX",
                                     "LINEITEM_RETURNFLAG", "LINEITEM_LINESTATUS", "LINEITEM_SHIPDATE", "LINEITEM_COMMITDATE",
                                     "LINEITEM_RECEIPTDATE", "LINEITEM_SHIPINSTRUCT", "LINEITEM_SHIPMODE", "LINEITEM_COMMENT"},

                        'PART': {"PART_PARTKEY", "PART_NAME", "PART_MFGR", "PART_BRAND", "PART_TYPE",
                                 "PART_SIZE", "PART_CONTAINER", "PART_RETAILPRICE", "PART_COMMENT"}},

          'binding_patterns': {2: 'PART', 5: 'PART'}},

  '18a' : {'stmts'    : {1:  {'map_vars': [("QUERY18_mLINEITEM2",            ["CUSTOMER_CUSTKEY", "QUERY18_mLINEITEMLINEITEM_ORDERKEY"]),
                                           ("QUERY18_mLINEITEM2_mCUSTOMER1", ["CUSTOMER_CUSTKEY", "QUERY18_mLINEITEMLINEITEM_ORDERKEY"])]},

                         2:  {'map_vars': [("QUERY18_mLINEITEM5", ["CUSTOMER_CUSTKEY", "O_ORDERKEY"]),
                                           ("QUERY18_mCUSTOMER1", ["CUSTOMER_CUSTKEY", "O_ORDERKEY"])]},

                         18: {'map_vars': [("QUERY18",                      ["C_CUSTKEY"]),
                                           ("QUERY18_mLINEITEM2",           ["C_CUSTKEY", "LINEITEM_ORDERKEY"]),
                                           ("QUERY18_mCUSTOMER1_L1_1_L1_1", ["LINEITEM_ORDERKEY"]),
                                           ("QUERY18_mCUSTOMER1_L1_2",      ["LINEITEM_ORDERKEY"])]},

                         19: {'map_vars': [("QUERY18_mLINEITEM5", ["C_CUSTKEY", "LINEITEM_ORDERKEY"]),
                                           ("QUERY18_mLINEITEM2", ["C_CUSTKEY", "LINEITEM_ORDERKEY"])]},

                         20: {'map_vars': [("QUERY18_mCUSTOMER1",            ["QUERY18_mCUSTOMERCUSTOMER_CUSTKEY", "LINEITEM_ORDERKEY"]),
                                           ("QUERY18_mLINEITEM2_mCUSTOMER1", ["QUERY18_mCUSTOMERCUSTOMER_CUSTKEY", "LINEITEM_ORDERKEY"])]}},

           'bindings': {'LINEITEM': {"LINEITEM_ORDERKEY", "LINEITEM_PARTKEY", "LINEITEM_SUPPKEY", "LINEITEM_LINENUMBER",
                                     "LINEITEM_QUANTITY", "LINEITEM_EXTENDEDPRICE", "LINEITEM_DISCOUNT", "LINEITEM_TAX",
                                     "LINEITEM_RETURNFLAG", "LINEITEM_LINESTATUS", "LINEITEM_SHIPDATE", "LINEITEM_COMMITDATE",
                                     "LINEITEM_RECEIPTDATE", "LINEITEM_SHIPINSTRUCT", "LINEITEM_SHIPMODE", "LINEITEM_COMMENT"},

                        'ORDERS'  : {"ORDERS_ORDERKEY", "ORDERS_CUSTKEY", "ORDERS_ORDERSTATUS", "ORDERS_TOTALPRICE",
                                     "ORDERS_ORDERDATE", "ORDERS_ORDERPRIORITY", "ORDERS_CLERK", "ORDERS_SHIPPRIORITY", "ORDERS_COMMENT"},

                        'CUSTOMER': {"CUSTOMER_CUSTKEY", "CUSTOMER_NAME", "CUSTOMER_ADDRESS", "CUSTOMER_NATIONKEY", "CUSTOMER_PHONE",
                                     "CUSTOMER_ACCTBAL", "CUSTOMER_MKTSEGMENT", "CUSTOMER_COMMENT"}
                       },

           'binding_patterns': {1: 'CUSTOMER', 2: 'CUSTOMER',
                                18: 'LINEITEM', 19: 'LINEITEM', 20: 'LINEITEM' }},

  '18' : {'stmts'    : {2:  {'map_vars': [("QUERY18_mLINEITEM1", ["O_ORDERKEY", "CUSTOMER_NAME", "CUSTOMER_CUSTKEY", "O_ORDERDATE", "O_TOTALPRICE"]),
                                          ("QUERY18_mCUSTOMER1", ["O_ORDERKEY", "CUSTOMER_CUSTKEY", "O_ORDERDATE", "O_TOTALPRICE"])]},

                        3:  {'map_vars': [("QUERY18_mLINEITEM1_mLINEITEM1", ["QUERY18_mLINEITEM1_mLINEITEMLINEITEM_ORDERKEY", "CUSTOMER_NAME", "CUSTOMER_CUSTKEY", "O_ORDERDATE", "O_TOTALPRICE"]),
                                          ("QUERY18_mCUSTOMER1_mLINEITEM1", ["QUERY18_mLINEITEM1_mLINEITEMLINEITEM_ORDERKEY", "CUSTOMER_CUSTKEY", "O_ORDERDATE", "O_TOTALPRICE"])]},

                        8:  {'map_vars': [("QUERY18",                      ["C_NAME", "ORDERS_CUSTKEY", "ORDERS_ORDERKEY", "ORDERS_ORDERDATE", "ORDERS_TOTALPRICE"]),
                                          ("QUERY18_mLINEITEM1_E1_1_L1_1", ["ORDERS_ORDERKEY"]),
                                          ("QUERY18_mLINEITEM1_E1_1_L1_1", ["ORDERS_ORDERKEY"]),
                                          ("QUERY18_mORDERS2",             ["C_NAME", "ORDERS_CUSTKEY"]),
                                          ("QUERY18_mLINEITEM1_E1_1_L1_1", ["ORDERS_ORDERKEY"]) ]},

                        11: {'map_vars': [("QUERY18_mLINEITEM1",           ["ORDERS_ORDERKEY", "C_NAME", "ORDERS_CUSTKEY", "ORDERS_ORDERDATE", "ORDERS_TOTALPRICE"]),
                                          ("QUERY18_mORDERS2",             ["C_NAME", "ORDERS_CUSTKEY"]),
                                          ("QUERY18_mLINEITEM1_E1_1_L1_1", ["ORDERS_ORDERKEY"])]},

                        12: {'map_vars': [("QUERY18_mLINEITEM1_mLINEITEM1", ["ORDERS_ORDERKEY", "C_NAME", "ORDERS_CUSTKEY", "ORDERS_ORDERDATE", "ORDERS_TOTALPRICE"]),
                                          ("QUERY18_mORDERS2",              ["C_NAME", "ORDERS_CUSTKEY"])]},

                        18: {'map_vars': [("QUERY18_mCUSTOMER1",            ["LINEITEM_ORDERKEY", "QUERY18_mCUSTOMERCUSTOMER_CUSTKEY", "O_ORDERDATE", "O_TOTALPRICE"]),
                                          ("QUERY18_mCUSTOMER1_mLINEITEM1", ["LINEITEM_ORDERKEY", "QUERY18_mCUSTOMERCUSTOMER_CUSTKEY", "O_ORDERDATE", "O_TOTALPRICE"])]},

                        19: {'map_vars': [("QUERY18_mLINEITEM1",            ["LINEITEM_ORDERKEY", "C_NAME", "C_CUSTKEY", "O_ORDERDATE", "O_TOTALPRICE"]),
                                          ("QUERY18_mLINEITEM1_mLINEITEM1", ["LINEITEM_ORDERKEY", "C_NAME", "C_CUSTKEY", "O_ORDERDATE", "O_TOTALPRICE"])] }},

           'bindings': {'LINEITEM': {"LINEITEM_ORDERKEY", "LINEITEM_PARTKEY", "LINEITEM_SUPPKEY", "LINEITEM_LINENUMBER",
                                     "LINEITEM_QUANTITY", "LINEITEM_EXTENDEDPRICE", "LINEITEM_DISCOUNT", "LINEITEM_TAX",
                                     "LINEITEM_RETURNFLAG", "LINEITEM_LINESTATUS", "LINEITEM_SHIPDATE", "LINEITEM_COMMITDATE",
                                     "LINEITEM_RECEIPTDATE", "LINEITEM_SHIPINSTRUCT", "LINEITEM_SHIPMODE", "LINEITEM_COMMENT"},

                        'ORDERS'  : {"ORDERS_ORDERKEY", "ORDERS_CUSTKEY", "ORDERS_ORDERSTATUS", "ORDERS_TOTALPRICE",
                                     "ORDERS_ORDERDATE", "ORDERS_ORDERPRIORITY", "ORDERS_CLERK", "ORDERS_SHIPPRIORITY", "ORDERS_COMMENT"},

                        'CUSTOMER': {"CUSTOMER_CUSTKEY", "CUSTOMER_NAME", "CUSTOMER_ADDRESS", "CUSTOMER_NATIONKEY", "CUSTOMER_PHONE",
                                     "CUSTOMER_ACCTBAL", "CUSTOMER_MKTSEGMENT", "CUSTOMER_COMMENT"}
                       },

          'binding_patterns': {2: 'CUSTOMER', 3: 'CUSTOMER',
                               8: 'ORDERS', 11: 'ORDERS', 12: 'ORDERS',
                               18: ':LINEITEM', 19: 'LINEITEM'}},

}


# Per-invocation globals.
query = ""
stmts = {}
buckets = {}
debug = False

pattern_map = {}

def init_pattern(query_id, as_routing):
  global query
  global stmts
  global buckets
  query = query_id
  valid = query in map_buckets_by_query and (not as_routing or query in stmts_by_query)
  if valid:
    buckets = map_buckets_by_query[query_id]
    if as_routing:
      stmts = stmts_by_query[query_id]

  return valid

# Returns all maps present in a statement's rhs.
def get_rhs_maps(stmt_id):
  return [rhs_map_name for (rhs_map_name, _) in stmts['stmts'][stmt_id]['map_vars'][1:]]

# Returns names and positions of free variables in an lhs map.
def get_free_lhs(stmt_id, bound_vars):
  (lhs_map_name, lhs_vars) = stmts['stmts'][stmt_id]['map_vars'][0]
  return (lhs_map_name, [(i,v) for (i,v) in enumerate(lhs_vars) if v not in bound_vars])

# Returns all variables present in a statement's rhs except for those in ignores.
def get_freebound_rhs(stmt_id, ignores):
  rhs_npv = {}
  for (rhs_map_name, rhs_vars) in stmts['stmts'][stmt_id]['map_vars'][1:]:
    rhs_npv[rhs_map_name] = [(i,v) for (i,v) in enumerate(rhs_vars) if v not in ignores]

  return rhs_npv

def rebuild_bucket(map_name, lb, rb, ridx, rpv):
  lidx = 0
  idx_set = {p for (p,_) in rpv}
  bucket = []
  for i in range(len(buckets['maps'][map_name][1])):
    if i in idx_set:
      bucket.append(rb[ridx[(map_name, i)]])
    else:
      bucket.append(lb[lidx])
      lidx += 1

  return bucket

def linearize(sizes, positions):
  idx = 0
  shift = 1
  for (sz, i) in zip(sizes, positions):
    idx += i * shift
    shift *= sz
  return idx

def k3tuple(t, collection):
  if len(t) == 1:
    return {'elem': t[0]} if collection else t[0]
  elif len(t) == 2:
    return {'key': t[0], 'value': t[1]}
  else:
    chars = string.ascii_lowercase
    l = int(math.ceil(float(len(t)) / len(chars)))
    k3t = {}
    charseqs = [''.join(comb) for n in range(1, l + 1) for comb in itertools.product(chars, repeat=n)]
    for (v,i) in zip(t, charseqs):
      k3t['r{}'.format(i)] = v

    return k3t

def generate_pattern(varname, stmt_id):
  global pattern_map

  bindings = stmts['bindings'][stmts['binding_patterns'][stmt_id]]

  (lhs_map_name, lhs_pv) = get_free_lhs(stmt_id, bindings)
  (lhs_map_id, lhs_bucket_sizes) = buckets['maps'][lhs_map_name]

  lhs_free_bs = [lhs_bucket_sizes[i] for (i,_) in lhs_pv]
  lhs_enums = [range(sz) for sz in lhs_free_bs]

  rhs_map_ids = get_rhs_maps(stmt_id)
  rhs_npv = get_freebound_rhs(stmt_id, {v for (_,v) in lhs_pv})

  # A list of (mapname, position) pairs for rhs free variables.
  rhs_uniqf_pos = list({(rhs_map_name, p) for (rhs_map_name, rhs_pv) in rhs_npv.items() for (p,v) in rhs_pv if v not in bindings})

  # A dict of varname => (mapname, position) pairs for rhs bound variables.
  rhs_uniqb_vars = {}
  for (rhs_map_name, rhs_pv) in rhs_npv.items():
    for (p,v) in rhs_pv:
      if v in bindings:
        if v not in rhs_uniqb_vars:
          rhs_uniqb_vars[v] = []
        rhs_uniqb_vars[v].append((rhs_map_name, p))

  rhs_bucket_sizes = []
  rhs_enum_idx = {}

  # Compute bucket sizes while accounting for repeated bound variables
  cnt = 0
  for (n,p) in rhs_uniqf_pos:
    rhs_enum_idx[(n,p)] = cnt
    rhs_bucket_sizes.append(buckets['maps'][n][1][p])
    cnt += 1

  for (v,np) in rhs_uniqb_vars.items():
    bs = 0
    for (n,p) in np:
      if bs == 0:
        bs = buckets['maps'][n][1][p]
      else:
        if bs != buckets['maps'][n][1][p]:
          raise ValueError("Bucket size mismatch on {}[{}]".format(n,v))
      rhs_enum_idx[(n,p)] = cnt
    rhs_bucket_sizes.append(bs)
    cnt += 1

  rhs_enums = [range(sz) for sz in rhs_bucket_sizes]

  if debug:
      print("LHS:\n" + "enums: {}".format(lhs_enums))
      print("RHS:\n" + '\n'.join(["npv:          {}".format(rhs_npv),
                              "uniqf_pos:    {}".format(rhs_uniqf_pos),
                              "uniqb_vars:   {}".format(rhs_uniqb_vars),
                              "enum_idx:     {}".format(rhs_enum_idx),
                              "bucket_sizes: {}".format(rhs_bucket_sizes)]))

  for lhs_bucket in itertools.product(*lhs_enums):
    ltuple = [linearize(lhs_free_bs, lhs_bucket)]
    if debug:
        print("LT : {}".format(ltuple))
        print("LB : {}".format(lhs_bucket))

    for rhs_bucket in itertools.product(*rhs_enums):
      if debug:
          print("RB : {}".format(rhs_bucket))
      tuple = list(ltuple)
      for map_name in rhs_map_ids:
        map_bucket = rebuild_bucket(map_name, lhs_bucket, rhs_bucket, rhs_enum_idx, rhs_npv[map_name])
        tuple.append(linearize(buckets['maps'][map_name][1], map_bucket))
        if debug:
            print("MB {}: {}".format(map_name, map_bucket))

      if rhs_bucket not in pattern_map:
        pattern_map[rhs_bucket] = []
      pattern_map[rhs_bucket].append(tuple)
      if debug:
          print("{} {}".format(rhs_bucket, tuple))

  # Generate pattern yaml.
  k3ds = []
  for (k,v) in sorted(pattern_map.items()):
    k3ds.append({'key' : k3tuple(k, collection=False),
                 'value' : [k3tuple(x, collection=True) for x in v]})

  k3n = varname + str(stmt_id)
  return {k3n: k3ds}

def get_pmap(query):
  available = init_pattern(query, False)
  if available:
    k3pmap = []
    for (n, spec) in buckets['maps'].items():
      dims = [{'key': i, 'value': sz} for (i,sz) in enumerate(spec[1])]
      k3pmap.append({'key': n, 'value': dims})
    return {'pmap_input': k3pmap}
  return None

def get_node_data(query, varname='route_opt_init_s', stmt_ids=None):
  nd = {}
  available = init_pattern(query, True)
  if available:
    if stmt_ids is None:
      stmt_ids = stmts['stmts'].keys()
    for (i,s) in enumerate(stmt_ids):
      nd.update(generate_pattern(varname, s))
    return nd
  return None

def main():
  global debug
  parser = argparse.ArgumentParser()
  parser.add_argument('--varname', metavar='VAR', default='route_opt_init_s', dest='varname', help='K3 variable name')
  parser.add_argument('--query', metavar='QUERY', type=str, required=True, dest='query', help='TPCH query number')
  parser.add_argument('--stmt', metavar='STMT', type=int, nargs='+', dest='stmts', help='Statement ids')
  parser.add_argument('--output', metavar='OUTPUT_FILE', dest='filename', help='Output file')
  parser.add_argument('--debug', default=False, action='store_true', help='Debug output')
  args = parser.parse_args()
  if args:
    if args.debug:
        debug = args.debug
    if args.stmts:
      nd = get_node_data(args.query, args.varname, args.stmts)
    else:
      nd = get_node_data(args.query, args.varname)

    nd.update(get_pmap(args.query))

    if args.filename:
      with open(args.filename, 'w') as f:
        f.write(yaml.dump(nd))
    else:
      print(yaml.dump(nd))

  else:
    parser.print_help()

if __name__ == '__main__':
    main()
