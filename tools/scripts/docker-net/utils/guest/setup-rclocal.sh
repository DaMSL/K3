#!/bin/bash

if [ "$#" -ne 1 ]; then
  echo "$0 <docker-net base>"
  exit 1
fi

DOCKERNET_BASE=$1
HOST=`hostname`

if [ ! -d $DOCKERNET_BASE/stage/rc.local ]; then
  echo "Invalid docker-net base directory"
  exit 1
fi

cp $DOCKERNET_BASE/stage/rc.local/rc.local.$HOST /etc/rc.local
