#!/bin/bash

# This script should be run in the docker-net base directory.

if [ "$#" -ne 2 ]; then
  echo "$0 <nodes file> <output file>"
  echo "This script creates a csv of <network device>, <MAC address>, <hostname> triples"
  exit 1
fi

OUT=$2
bin/sshsudo $1 ip -o link show vdocker0 | grep "vdocker0@" | awk '{print $2, $16}' | sed "s/vdocker0@//g; s/:\ /,/g" > $OUT.tmp
paste -d, $OUT.tmp $1 > $OUT
rm $OUT.tmp
