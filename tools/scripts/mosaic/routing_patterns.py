#!/usr/bin/env python

import argparse, itertools, math, string, sys, yaml

map_buckets_by_query = {
  4: {'maps': { 'ORDER_COUNT'               : (1, [8]),
                'ORDER_COUNT_mLINEITEM1'    : (2, [8,8]),
                'ORDER_COUNT_mORDERS3_E1_1' : (3, [8]) }},

  3: {'maps': { "QUERY3"                        : (1, [8, 8, 8]),
                "QUERY3_mLINEITEM1"             : (2, [8, 8, 8]),
                "QUERY3_mLINEITEM1_mCUSTOMER2"  : (3, [8, 8, 8, 8]),
                "QUERY3_mORDERS1"               : (4, [8]),
                "QUERY3_mORDERS3"               : (5, [8]),
                "QUERY3_mORDERS6"               : (6, [8]),
                "QUERY3_mCUSTOMER2"             : (7, [8, 8, 8, 8]),
                "QUERY3_mCUSTOMER4"             : (8, [8, 8, 8, 8]),
              }},

  10: {'maps': { "REVENUE"                       : (2, [8, 8, 8, 8, 8, 8, 8]),
                 "REVENUE_mLINEITEM2"            : (3, [8, 8, 8, 8, 8, 8, 8, 8]),
                 "REVENUE_mLINEITEM2_mCUSTOMER1" : (4, [8, 8]),
                 "REVENUE_mORDERS1"              : (5, [8]),
                 "REVENUE_mORDERS4"              : (6, [8, 8, 8, 8, 8, 8, 8]),
                 "REVENUE_mORDERS5"              : (7, [8]),
                 "REVENUE_mCUSTOMER1"            : (8, [8]),
                 "REVENUE_mCUSTOMER2"            : (9, [8, 8]),
                 "REVENUE_mCUSTOMER3"            : (10, [8]),
      }}
}

stmts_by_query = {
	4 : {'stmts':
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


  3 : {'stmts':
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

  10 : {'stmts': {
          16: [("REVENUE",            ["C_CUSTKEY", "C_NAME", "C_ACCTBAL", "N_NAME", "C_ADDRESS", "C_PHONE", "C_COMMENT"]),
               ("REVENUE_mLINEITEM2", ["C_CUSTKEY", "C_NAME", "C_ACCTBAL", "N_NAME", "C_ADDRESS", "C_PHONE", "C_COMMENT", "LINEITEM_ORDERKEY"])],

          19: [("REVENUE_mCUSTOMER1",            ["REVENUE_mCUSTOMERCUSTOMER_CUSTKEY"]),
               ("REVENUE_mLINEITEM2_mCUSTOMER1", ["REVENUE_mCUSTOMERCUSTOMER_CUSTKEY", "LINEITEM_ORDERKEY"])],

          20: [("REVENUE_mCUSTOMER3",            ["REVENUE_mCUSTOMERCUSTOMER_CUSTKEY"]),
               ("REVENUE_mLINEITEM2_mCUSTOMER1", ["REVENUE_mCUSTOMERCUSTOMER_CUSTKEY", "LINEITEM_ORDERKEY"])],

          6: [("REVENUE",          ["ORDERS_CUSTKEY", "C_NAME", "C_ACCTBAL", "N_NAME", "C_ADDRESS", "C_PHONE", "C_COMMENT"]),
              ("REVENUE_mORDERS4", ["ORDERS_CUSTKEY", "C_NAME", "C_ACCTBAL", "N_NAME", "C_ADDRESS", "C_PHONE", "C_COMMENT"]),
              ("REVENUE_mORDERS1", ["ORDERS_ORDERKEY"]),
              ("REVENUE_mORDERS5", ["ORDERS_ORDERKEY"])],

          7: [("REVENUE_mLINEITEM2", ["ORDERS_CUSTKEY", "C_NAME", "C_ACCTBAL", "N_NAME", "C_ADDRESS", "C_PHONE", "C_COMMENT", "ORDERS_ORDERKEY"]),
              ("REVENUE_mORDERS4",   ["ORDERS_CUSTKEY", "C_NAME", "C_ACCTBAL", "N_NAME", "C_ADDRESS", "C_PHONE", "C_COMMENT"])],

          0: [("REVENUE",            ["CUSTOMER_CUSTKEY", "CUSTOMER_NAME", "CUSTOMER_ACCTBAL", "N_NAME", "CUSTOMER_ADDRESS", "CUSTOMER_PHONE", "CUSTOMER_COMMENT"]),
              ("REVENUE_mCUSTOMER2", ["N_NAME", "CUSTOMER_NATIONKEY"]),
              ("REVENUE_mCUSTOMER1", ["CUSTOMER_CUSTKEY"]),
              ("REVENUE_mCUSTOMER3", ["CUSTOMER_CUSTKEY"])],

          2: [("REVENUE_mORDERS4",   ["CUSTOMER_CUSTKEY", "CUSTOMER_NAME", "CUSTOMER_ACCTBAL", "N_NAME", "CUSTOMER_ADDRESS", "CUSTOMER_PHONE", "CUSTOMER_COMMENT"]),
              ("REVENUE_mCUSTOMER2", ["N_NAME", "CUSTOMER_NATIONKEY"])]
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
          6: 'ORDERS', 7: 'ORDERS'
          0: 'CUSTOMER', 2: 'CUSTOMER'
        }}
}

# Per-invocation globals.
query = 0
stmts = {}
buckets = {}

pattern_map = {}

def init_pattern(query_id):
  global query
  global stmts
  global buckets
  query = query_id
  stmts = stmts_by_query[query_id]
  buckets = map_buckets_by_query[query_id]
  print ("Processing pattern for query: " + str(query_id))

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

def k3tuple(t):
  chars = string.ascii_lowercase
  l = int(math.ceil(float(len(t)) / len(chars)))
  k3t = {}
  charseqs = [''.join(comb) for n in range(1, l + 1) for comb in itertools.product(chars, repeat=n)]
  for (v,i) in zip(t, charseqs):
    k3t['r{}'.format(i)] = v

  return k3t

def generate_pattern(varname, filename, stmt_id):
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
    rhs_enum_idx[k] = cnt
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

  print("LHS:\n" + str(lhs_enums))
  print("RHS:\n" + '\n'.join([str(rhs_npv), str(rhs_uniqf_pos), str(rhs_uniqb_vars), str(rhs_enum_idx), str(rhs_bucket_sizes)]))

  for lhs_bucket in itertools.product(*lhs_enums):
    ltuple = [linearize(lhs_free_bs, lhs_bucket)]
    # print("LT : {}".format(ltuple))
    # print("LB : {}".format(lhs_bucket))

    for rhs_bucket in itertools.product(*rhs_enums):
      # print("RB : {}".format(rhs_bucket))
      tuple = list(ltuple)
      for map_name in rhs_map_ids:
        map_bucket = rebuild_bucket(map_name, lhs_bucket, rhs_bucket, rhs_enum_idx, rhs_npv[map_name])
        tuple.append(linearize(buckets['maps'][map_name][1], map_bucket))
        # print("MB {}: {}".format(map_name, map_bucket))

      if rhs_bucket not in pattern_map:
        pattern_map[rhs_bucket] = []
      pattern_map[rhs_bucket].append(tuple)
      # print("{} {}".format(rhs_bucket, tuple))

  k3ds = []
  for (k,v) in sorted(pattern_map.items()):
    k3ds.append({'key' : k3tuple(k), 'value' : [k3tuple(x) for x in v]})

  k3n = varname + str(stmt_id)
  k3nds = {k3n: k3ds}

  # print(yaml.dump(k3nds))
  with open(filename, 'w') as f:
    f.write(yaml.dump(k3nds))

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--varname', metavar='VAR', default='route_opt_init_s', dest='varname', help='K3 variable name')
  parser.add_argument('--query', metavar='QUERY', type=int, required=True, dest='query', help='TPCH query number')
  parser.add_argument('--stmt', metavar='STMT', type=int, required=True, dest='stmt', help='Statement id')
  parser.add_argument('--output', metavar='OUTPUT_FILE', required=True, dest='filename', help='Output file')
  args = parser.parse_args()
  if args:
    init_pattern(args.query)
    generate_pattern(args.varname, args.filename, args.stmt)
  else:
    parser.print_help()

if __name__ == '__main__':
    main()
