#!/bin/bash
# run from k3 base directory

set -e

if [  $# -ne 3 ]
then
  echo "usage: $0 path/to/k3/source path/to/run/yaml path/to/correct/csv"

  exit 1
fi

SOURCE=$1
YAML=$2
CORRECT=$3

# remove old executables
./tools/scripts/run/clean.sh &> /dev/null || true

# compile the k3 executable
echo "Compiling $SOURCE ..."
./tools/scripts/run/compile.sh $SOURCE &> /dev/null || echo "Failed"

# run the k3 executable
echo "Running $YAML ..."
__build/A -p $YAML &> /dev/null || echo "Failed"

# diff results
echo "Comparing results ..."
python2 tools/ktrace/csv_diff.py $CORRECT results.csv && echo "Success"
