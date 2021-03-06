#!/bin/bash

# Run from top level k3 directory
sqlite3 temp.db < tools/ktrace/schema.sql
sqlite3 temp.db < tools/ktrace/load_tiny.sql

for query in tpch_q1 tpch_q3 tpch_q4 tpch_q5 tpch_q6 tpch_q11 tpch_q12 tpch_q14 tpch_q15 tpch_q17 tpch_q18 tpch_q19 tpch_q20 tpch_q22 amplab_q1 amplab_q2 amplab_q3;
do
  sqlite3 temp.db < tools/ktrace/queries/$query/query.sql > tools/ktrace/queries/$query/correct.csv
done
