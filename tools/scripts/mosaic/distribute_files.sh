#!/bin/bash
# distribute files among nodes
if [[ $# -ne 1 ]]
then
  echo "Please provide path to copy over"
  exit 1
fi

nodes='3 4 5 6 -hd1 -hd2 -hd3 -hd4 -hd5 -hd6 -hd7 -hd9 -hd10 -hd11 -hd12 -hd13 -hd14 -hd15 -hd16 -hm1 -hm2 -hm3 -hm4 -hm5 -hm6 -hm7 -hm8'
host=`hostname`

for n in $nodes; do
  if [[ "$host" != "qp$n" ]]
  then
    dest=`dirname $1`
    cmd="rsync -ave ssh --rsync-path=/usr/bin/rsync $1 qp$n:$dest"
    echo $cmd
    `$cmd`
  fi
done

