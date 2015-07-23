#!/bin/bash
# run from k3 base directory

if [  $# -ne 3 ]
then
  echo "usage: $0 path/to/k3/source path/to/run/yaml path/to/correct/csv"

  exit 1
fi

SOURCE=$1
YAML=$2
CORRECT=$3

# remove old executables/results
./tools/scripts/run/clean.sh &> /dev/null
rm results.csv &> /dev/null

# compile the k3 executable
echo "Compiling $SOURCE ..."
./tools/scripts/run/compile.sh $SOURCE &> /dev/null
if [ $? -ne 0 ]; then
    echo "Failed"
    exit 1
fi

# run the k3 executable
echo "Running $YAML ..."
__build/A -p $YAML &> /dev/null
if [ $? -ne 0 ]; then
    echo "Failed"
    exit 1
fi

# diff results
echo "Comparing results ..."
python2 tools/ktrace/csv_diff.py $CORRECT results.csv && echo "Success"
if [ $? -ne 0 ]; then
    echo "Failed"
    exit 1
fi
