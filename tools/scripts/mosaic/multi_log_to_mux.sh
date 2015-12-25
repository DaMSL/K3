if [  $# -le 1 ] 
then
  echo "Usage: $0 <log_file.txt> <output_folder>"
  exit 1
fi

LOGF=$1
OUTF=$2

for sn in 1 2 4 8 16 32 64;
do
  mkdir -p $OUTF/$sn
  ruby log_to_mux.rb $LOGF $sn $OUTF/$sn/
done
