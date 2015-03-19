#!/bin/bash

# Run from root K3 directory
export SNDPATH=`find .cabal-sandbox -name "*packages.conf.d"`
./dist/build/k3/k3 -I ./lib/k3 -I ./examples/sql compile -l ktrace $@  --mpargs package-db=$SNDPATH --mpsearch ./src
