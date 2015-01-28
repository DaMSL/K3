if [  $# -ne 3 ] 
then 
  echo "Usage: $0 k3_source test_dir results_dir" 
  exit 1
fi 

INFILE=$(pwd)/$1
TESTDIR=$(pwd)/$2
RESULTVAR=$3
K3DIR=~/k3/
DRIVER=~/k3/K3-Driver
KTRACE=$(pwd)

# Remove old executables
rm $DRIVER/__build/A $DRIVER/__build/__build/*

# Compile the K3 executable
cd $DRIVER && ./compile.sh $INFILE

# Create a directory for K3 Results
cd $KTRACE && mkdir $TESTDIR/results && rm $TESTDIR/results/*

# Run the K3 Executable, stash results
$DRIVER/__build/A -p $TESTDIR/peers.yaml --result_var $RESULTVAR --result_path $TESTDIR/results/

# Run KTrace to populate the database with K3 Results
python driver.py $K3DIR $INFILE $RESULTVAR $TESTDIR/results/  

# Compute the correct results
psql -f $TESTDIR/correctResult.sql

# Compute the diff, ensure 0 rows are produced
psql -c 'select * from compute_diff;' | grep "(0 rows)" 
