#!/bin/bash
# Pass in a directory containing the following files:
# run_pg_query.sql - Load Data into Postgres, run query, store result in table: pg_result
# load_k3_results.sql - Load the result of a K3 SQL query into Postgres table: k3_result
# diff.sql - Compute a diff on tables pg_result and k3_result. Should return 0 rows upon success.
sudo -i -u postgres psql -f $1/run_pg_query.sql
sudo -i -u postgres psql -f $1/load_k3_results.sql
if $(sudo -i -u postgres psql -f $1/diff.sql | grep -q "0 rows");
then
 echo Success
 exit 0
else
  echo Failure
  exit -1
fi
