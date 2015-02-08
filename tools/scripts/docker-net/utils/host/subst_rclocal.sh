#!/bin/bash

# This script should be run in the docker-net base directory.

if [ "$#" -ne 1 ]; then
  echo "$0 <dev-addr-ip file>"
  exit 1
fi

for i in `cat $1`; do
  dev=`echo $i | sed "s/\([^,]*\),.*/\1/g"`
  macaddr=`echo $i | sed "s/[^,]*,\([^,]*\),.*/\1/g"`
  ip=`echo $i | sed "s/[^,]*,[^,]*,\(.*\)/\1/g"`

  sed "s/@SRCDEV@/$dev/g; s/@MACADDR@/$macaddr/g" < stubs/rc.local > stage/rc.local/rc.local.$ip
done
