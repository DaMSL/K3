#!/bin/bash
# Run from K3 Base directory

set -e

if [  $# -ne 3 ] 
then 
  echo "Usage: $0 k3_source test_dir results_var" 
  exit 1
fi 

INFILE=$1
TESTDIR=$2
RESULTVAR=$3
KTRACE=./tools/ktrace

# Remove old executables
rm __build/A __build/__build/* || true

# Compile the K3 executable
./tools/scripts/run/compile.sh $INFILE

# Create a directory for K3 Results
mkdir $TESTDIR/results || rm $TESTDIR/results/* || true

# Run the K3 Executable, stash results
__build/A -p $TESTDIR/peers.yaml --result_var $RESULTVAR --result_path $TESTDIR/results/

# Run KTrace to populate the database with K3 Results
python $KTRACE/driver.py $(pwd) $INFILE $RESULTVAR $(pwd)/$TESTDIR/results/ | psql

# Compute the correct results
psql -f $(pwd)/$TESTDIR/correctResult.sql

# Compute the diff, ensure 0 rows are produced
psql -c 'select * from compute_diff;' | grep "(0 rows)"
echo "SUCCESS"
