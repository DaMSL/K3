#!/bin/bash

#get directory of script, handling links etc
pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd -P`
popd > /dev/null

for i in 1 3 4 6 10 11a 12 13 14 17 18 18a 19 22a; do "$SCRIPTPATH/run.rb" -w tpch$i -k /local/data/mosaic/sf-0.01/vanilla/agenda.tbl --debug --csv-data ../K3-Mosaic/tests/queries/tpch/query$i.sql -1 -3 -4; done &> errors.txt

