#!/bin/bash

set -e

if [  $# -ne 3 ] 
then 
  echo "Usage: $0 k3_source test_dir results_var" 
  exit 1
fi 

INFILE=$(pwd)/$1
TESTDIR=$(pwd)/$2
RESULTVAR=$3
KTRACE=$(pwd)

source env.sh

# Remove old executables
rm $DRIVER/__build/A $DRIVER/__build/__build/* || true

# Compile the K3 executable
cd $DRIVER && ./compile.sh $INFILE

# Create a directory for K3 Results
cd $KTRACE && mkdir $TESTDIR/results || rm $TESTDIR/results/* || true

# Run the K3 Executable, stash results
$DRIVER/__build/A -p $TESTDIR/peers.yaml --result_var $RESULTVAR --result_path $TESTDIR/results/

# Run KTrace to populate the database with K3 Results
python driver.py $K3DIR $INFILE $RESULTVAR $TESTDIR/results/ | psql

# Compute the correct results
psql -f $TESTDIR/correctResult.sql

# Compute the diff, ensure 0 rows are produced
psql -c 'select * from compute_diff;' | grep "(0 rows)"
echo "SUCCESS"
