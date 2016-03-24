#!/usr/bin/env python

import argparse, itertools, math, string, sys, yaml

# Notes:
# There are 3 classes of variables in a statement: bound, common free, and unique free. Common free
# variables are used in both the rhs and lhs and are therefore 'synchronized'. Unique free variables
# can only occur on the rhs. With each class, we can consider the possibility that we vary the number
# of buckets. Varying bound variables requires applying the highest bucket value, and calculating the result
# of that bucket (lower buckets would map to a lower value). It probably requires placing the maximum value
# in the table as well, so the program knows to use it.
# Varying unique free variables is easy, since it has minimal impact, so long as the same position isn't
# in a different class in another statement, which it probably is. Varying common free variables, which
# are both on the lhs and rhs, is doable. It involves calculating the number of messages that will occur
# when a lower-bucket-size position is sent to a higher-bucket-size position, or vice versa. This is just
# cyclic groups, and is fairly easy to solve.

## Template
# 'x' : {'maps' : {i: ()} }

map_buckets_by_query = {
  '3': {'maps': { "QUERY3"                        : (1, [4, 4, 4]),
                  "QUERY3_mLINEITEM1"             : (2, [4, 4, 4]),
                  "QUERY3_mLINEITEM1_mCUSTOMER2"  : (3, [4, 4, 4, 4]),
                  "QUERY3_mORDERS1"               : (4, [16]), # not optroute
                  "QUERY3_mORDERS3"               : (5, [16]), # not optroute
                  "QUERY3_mORDERS6"               : (6, [16]), # not optroute
                  "QUERY3_mCUSTOMER2"             : (7, [4, 4, 4, 4]),
                  "QUERY3_mCUSTOMER4"             : (8, [4, 4, 4, 4]) }},

  '4': {'maps': { 'ORDER_COUNT'               : (1, [16]),
                  'ORDER_COUNT_mLINEITEM1'    : (2, [16, 16]),
                  'ORDER_COUNT_mORDERS3_E1_1' : (3, [16]) }},

  '10': {'maps': { "REVENUE"                       : (2, [8, 8, 8, 8, 8, 8, 8]),
                   "REVENUE_mLINEITEM2"            : (3, [8, 8, 8, 8, 8, 8, 8, 8]),
                   "REVENUE_mLINEITEM2_mCUSTOMER1" : (4, [8, 8]),
                   "REVENUE_mORDERS1"              : (5, [8]),
                   "REVENUE_mORDERS4"              : (6, [8, 8, 8, 8, 8, 8, 8]),
                   "REVENUE_mORDERS5"              : (7, [8]),
                   "REVENUE_mCUSTOMER1"            : (8, [8]),
                   "REVENUE_mCUSTOMER2"            : (9, [8, 8]),
                   "REVENUE_mCUSTOMER3"            : (10, [8]) }},

  '11a': {'maps': { "QUERY11A"            : (1, [16]),
                    "QUERY11A_mSUPPLIER1" : (2, [16, 16]),
                    "QUERY11A_mPARTSUPP1" : (3, [16]) }}, # not optroute

  '12': {'maps': { "HIGH_LINE_COUNT"            : (1, [16]),
                   "HIGH_LINE_COUNT_mLINEITEM1" : (2, [16]), # not optroute
                   "HIGH_LINE_COUNT_mLINEITEM8" : (3, [16]), # not optroute
                   "HIGH_LINE_COUNT_mORDERS1"   : (4, [16, 16]),
                   "HIGH_LINE_COUNT_mORDERS4"   : (5, [16, 16]),
                   "LOW_LINE_COUNT"             : (6, [16]),
                   "LOW_LINE_COUNT_mLINEITEM6"  : (7, [16]) }}, # not optroute
  # 13 has none
  # 15 has none

  '17': {'maps': { "AVG_YEARLY_pLINEITEM5"           : (2, [16, 16]),
                   "AVG_YEARLY_mLINEITEM1"           : (3, [16]), # not optroute
                   "AVG_YEARLY_mLINEITEM2_L1_1_L1_1" : (4, [16]), # not optroute
                   "AVG_YEARLY_mLINEITEM2_L1_2"      : (5, [16]), # not optroute
                   "AVG_YEARLY_mLINEITEM5"           : (6, [16, 16]) }},

  '18a': {'maps': { "QUERY18"                       : (1, [16]),
                    "QUERY18_mLINEITEM2"            : (2, [16, 16]),
                    "QUERY18_mLINEITEM2_mCUSTOMER1" : (3, [16, 16]),
                    "QUERY18_mLINEITEM5"            : (4, [16, 16]),
                    "QUERY18_mORDERS2"              : (5, [16]), # not optroute
                    "QUERY18_mCUSTOMER1"            : (6, [16, 16]),
                    "QUERY18_mCUSTOMER1_L1_1_L1_1"  : (7, [16]),
                    "QUERY18_mCUSTOMER1_L1_2"       : (8, [16]) }},

  '18': {'maps': { "QUERY18"                       : (1, [4, 4, 4, 4, 4]),
                   "QUERY18_mORDERS2"              : (2, [4, 4]),
                   "QUERY18_mCUSTOMER1"            : (3, [4, 4, 4, 4]),
                   "QUERY18_mCUSTOMER1_mLINEITEM1" : (4, [4, 4, 4, 4]),
                   "QUERY18_mLINEITEM1"            : (5, [4, 4, 4, 4, 4]),
                   "QUERY18_mLINEITEM1_mLINEITEM1" : (6, [4, 4, 4, 4, 4]),
                   "QUERY18_mLINEITEM1_E1_1_L1_1"  : (7, [4]) }},
  # 19 has none

  '22a': {'maps': {"QUERY22"                 : (1, [8]),
                   "QUERY22_mCUSTOMER1"      : (2, [8, 8, 8]),
                   "QUERY22_mCUSTOMER1_L2_1" : (4, [8]),
                   "QUERY22_mCUSTOMER1_L3_1" : (4, [8]) }}
}


## Template
# 'x' : {'stmts'    : {i: {'map_vars': [lmap, rmaps]}},
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

  '22a' : {'stmts'    : {6: {'map_vars': [("QUERY22",                 ["C1_NATIONKEY"]),
                                          ("QUERY22_mCUSTOMER1",      ["C1_NATIONKEY", "ORDERS_CUSTKEY", "C1_ACCTBAL"]),
                                          ("QUERY22_mCUSTOMER1_L3_1", ["ORDERS_CUSTKEY"])]}},

           'bindings' : {'ORDERS'  : {"ORDERS_ORDERKEY", "ORDERS_CUSTKEY", "ORDERS_ORDERSTATUS", "ORDERS_TOTALPRICE",
                                     "ORDERS_ORDERDATE", "ORDERS_ORDERPRIORITY", "ORDERS_CLERK", "ORDERS_SHIPPRIORITY", "ORDERS_COMMENT"},

                        'CUSTOMER': {"CUSTOMER_CUSTKEY", "CUSTOMER_NAME", "CUSTOMER_ADDRESS", "CUSTOMER_NATIONKEY", "CUSTOMER_PHONE",
                                     "CUSTOMER_ACCTBAL", "CUSTOMER_MKTSEGMENT", "CUSTOMER_COMMENT"}
                        },

           'binding_patterns': {6: 'ORDERS'}},
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

# Returns names and positions of bound variables in an lhs map.
def get_bound_lhs(stmt_id, bound_vars):
  lhs_vars = stmts['stmts'][stmt_id]['map_vars'][0][1]
  return [(i,v) for (i,v) in enumerate(lhs_vars) if v in bound_vars]

# Returns all variables present in a statement's rhs except for those in ignores.
def get_freebound_rhs(stmt_id, ignores):
  rmap_pv_not_free_lhs = {}
  for (rhs_map_name, rhs_vars) in stmts['stmts'][stmt_id]['map_vars'][1:]:
    rmap_pv_not_free_lhs[rhs_map_name] = [(i,v) for (i,v) in enumerate(rhs_vars) if v not in ignores]

  return rmap_pv_not_free_lhs

def rebuild_lhs_bucket(map_name, bb, bidx, lfb, lfpv):
  lidx = 0
  idx_set = {p for (p,_) in lfpv}
  bucket = []
  for i in range(len(buckets['maps'][map_name][1])):
    if i in idx_set:
      bucket.append(lfb[lidx])
      lidx += 1
    else:
      bucket.append(bb[bidx[(map_name, i)]])
  return bucket

def rebuild_rhs_bucket(map_name, bb, bidx, lb, rb, ridx):
  lidx = 0
  bucket = []
  for i in range(len(buckets['maps'][map_name][1])):
    if (map_name, i) in bidx:
      bucket.append(bb[bidx[(map_name, i)]])
    elif (map_name, i) in ridx:
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

# map from rmod to lmod, returning a mapping of rbucket -> lbucket set
def find_mod_pats(lmod, rmod):
  # normally we need to go up to the lowest common multiple, but we use the fact that
  # all our bucket dimensions are powers of 2 to simplify it
  route = {}
  max_mod = max(lmod, rmod)
  for i in range(max_mod):
      if route.has_key(i % rmod):
          route[i % rmod].add(i % lmod)
      else:
          route[i % rmod] = set([i % lmod])
  return route

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

# generate an input data structure for the requested stmt
# pv = (position, variable)
# bs = bucket_sizes
def generate_pattern(varname, stmt_id):
  global pattern_map

  bindings = stmts['bindings'][stmts['binding_patterns'][stmt_id]]

  # get lhs name and free vars
  (lhs_map_name, lhs_free_pv) = get_free_lhs(stmt_id, bindings)
  (lhs_map_id, lhs_bucket_sizes) = buckets['maps'][lhs_map_name]

  lhs_bound_pv = get_bound_lhs(stmt_id, bindings)

  lhs_free_bs = [lhs_bucket_sizes[i] for (i,_) in lhs_free_pv]
  # enumerate the possible indices for the buckets in each free variable
  lhs_free_enums = [range(sz) for sz in lhs_free_bs]

  rhs_map_ids = get_rhs_maps(stmt_id)
  # get map -> rhs vars not free in lmap (free + bound)
  rmap_pv_not_free_lhs = get_freebound_rhs(stmt_id, {v for (_,v) in lhs_free_pv})

  # extract a list of (mapname, position) pairs for rhs free variables that don't
  #   exist in the lhs
  r_uniq_free_map_pos = list({(rhs_map_name, p) \
    for (rhs_map_name, rhs_pv) in rmap_pv_not_free_lhs.items() \
      # filter out bound variables
      for (p,v) in rhs_pv if v not in bindings})

  rhs_uniq_free_bucket_sizes = []
  rhs_uniq_free_enum_idx = {}

  # Compute bucket sizes while accounting for repeated bound variables
  cnt = 0
  for (map,pos) in r_uniq_free_map_pos:
    rhs_uniq_free_enum_idx[(map,pos)] = cnt
    # append bucket size for this (map,pos)
    rhs_uniq_free_bucket_sizes.append(buckets['maps'][map][1][pos])
    cnt += 1

  rhs_uniq_free_enums = [range(sz) for sz in rhs_uniq_free_bucket_sizes]

  # A dict of varname => (mapname, position) pairs for all bound
  # variables occurrences in either lhs or rhs maps.
  uniq_bound_var_map_pos = {}

  # add the lhs bound vars
  for (p,v) in lhs_bound_pv:
    if v not in uniq_bound_var_map_pos:
      uniq_bound_var_map_pos[v] = []
    uniq_bound_var_map_pos[v].append((lhs_map_name, p))

  # add the rhs bound vars
  for (rhs_map_name, rhs_pv) in rmap_pv_not_free_lhs.items():
    for (p,v) in rhs_pv:
      if v in bindings:
        if v not in uniq_bound_var_map_pos:
          uniq_bound_var_map_pos[v] = []
        uniq_bound_var_map_pos[v].append((rhs_map_name, p))

  bound_bucket_sizes = []
  bound_enum_idx = {}
  bound_cnt = 0

  for v in bindings:
    bs = 0
    if v in uniq_bound_var_map_pos:
      for (map,pos) in uniq_bound_var_map_pos[v]:
        if bs == 0:
          bs = buckets['maps'][map][1][pos]
        else:
          if bs != buckets['maps'][map][1][pos]:
            raise ValueError("Bucket size mismatch on {}[{}]".format(map,v))
        bound_enum_idx[(map,pos)] = bound_cnt
      bound_bucket_sizes.append(bs)
      bound_cnt += 1

  bound_enums = [range(sz) for sz in bound_bucket_sizes]

  if debug:
      print("Bound:\n" + '\n'.join(["uniq_bound_var_map_pos:   {}".format(uniq_bound_var_map_pos),
                                    "bound_enum_idx:     {}".format(bound_enum_idx),
                                    "bound_bucket_sizes: {}".format(bound_bucket_sizes)]))

      print("LHS:\n" + "enums: {}".format(lhs_free_enums))

      print("RHS:\n" + '\n'.join(["npv:          {}".format(rmap_pv_not_free_lhs),
                                  "uniqf_pos:    {}".format(r_uniq_free_map_pos),
                                  "enum_idx:     {}".format(rhs_uniq_free_enum_idx),
                                  "bucket_sizes: {}".format(rhs_uniq_free_bucket_sizes)]))

  # iterate over cartesian product of bound bucket values
  for bound_bucket in itertools.product(*bound_enums):
    # iterate over cartesian product of lhs free vars
    for lhs_bucket in itertools.product(*lhs_free_enums):
      lhs_map_bucket = rebuild_lhs_bucket(
        lhs_map_name, bound_bucket, bound_enum_idx, lhs_bucket, lhs_free_pv)
      ltuple = [linearize(buckets['maps'][lhs_map_name][1], lhs_map_bucket)]
      if debug:
          print("LT : {}".format(ltuple))
          print("LB : {}".format(lhs_bucket))

      # add the rhs contributions to our tuple
      for rhs_bucket in itertools.product(*rhs_uniq_free_enums):
        if debug:
            print("RB : {}".format(rhs_bucket))
        tuple = list(ltuple)
        for map_name in rhs_map_ids:
          map_bucket = rebuild_rhs_bucket(
            map_name, bound_bucket, bound_enum_idx, lhs_bucket, rhs_bucket, rhs_uniq_free_enum_idx)
          tuple.append(linearize(buckets['maps'][map_name][1], map_bucket))
          if debug:
              print("MB {}: {}".format(map_name, map_bucket))

        if bound_bucket not in pattern_map:
          pattern_map[bound_bucket] = []
        pattern_map[bound_bucket].append(tuple)
        if debug:
            print("{} {}".format(bound_bucket, tuple))

  # Generate pattern yaml.
  k3ds = []
  for (k,v) in sorted(pattern_map.items()):
    k3ds.append({'key' : k3tuple(k, collection=False),
                 'value' : [k3tuple(x, collection=True) for x in v]})

  k3n = varname + str(stmt_id)
  pattern_map = {}
  return {k3n: k3ds}

# get the partition map definition
def get_pmap(query):
  available = init_pattern(query, False)
  if available:
    k3pmap = []
    for (n, spec) in buckets['maps'].items():
      dims = [{'key': i, 'value': sz} for (i,sz) in enumerate(spec[1])]
      k3pmap.append({'key': n, 'value': dims})
    return {'pmap_input': k3pmap}
  return None

# return routing data structure
def get_node_data(query, varname='route_opt_init_s', stmt_ids=None):
  nd = {}
  available = init_pattern(query, True)
  if available:
    if stmt_ids is None:
      stmt_ids = stmts['stmts'].keys()
    for s in stmt_ids:
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
