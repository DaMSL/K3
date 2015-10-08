#! /bin/sh
# Command for compiling cpp file in k3
# Since it's a little tricky to come up with the full command line, you can just use this
# You might need to adjust it to suit your system
export SNDPATH=`find .cabal-sandbox -name "*packages.conf.d"`

dist/build/k3/k3 \
  -I lib/k3 -I examples/sql -I examples/distributed/amplab/compact \
  --mpargs package-db=$SNDPATH --mpsearch src \
  sql $@
