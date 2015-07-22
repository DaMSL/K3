#!/bin/bash
# run from k3 base directory

set -e
./tools/ktrace/run_test.sh examples/sql/tpch/queries/k3/q1.k3 tools/ktrace/yaml/tpch.yaml tools/ktrace/queries/tpch_q1/correct.csv
./tools/ktrace/run_test.sh examples/sql/tpch/queries/k3/barrier-queries/q3.k3 tools/ktrace/yaml/tpch.yaml tools/ktrace/queries/tpch_q3/correct.csv
./tools/ktrace/run_test.sh examples/sql/tpch/queries/k3/barrier-queries/q5_bushy_broadcast_broj2.k3 tools/ktrace/yaml/tpch.yaml tools/ktrace/queries/tpch_q5/correct.csv
./tools/ktrace/run_test.sh examples/sql/tpch/queries/k3/q6.k3 tools/ktrace/yaml/tpch.yaml tools/ktrace/queries/tpch_q6/correct.csv
./tools/ktrace/run_test.sh examples/sql/tpch/queries/k3/barrier-queries/q18.k3 tools/ktrace/yaml/tpch.yaml tools/ktrace/queries/tpch_q18/correct.csv
./tools/ktrace/run_test.sh examples/sql/tpch/queries/k3/barrier-queries/q22.k3 tools/ktrace/yaml/tpch.yaml tools/ktrace/queries/tpch_q22/correct.csv

./tools/ktrace/run_test.sh examples/distributed/amplab/compact/q1.k3 tools/ktrace/yaml/amplab.yaml tools/ktrace/queries/amplab_q1/correct.csv
./tools/ktrace/run_test.sh examples/distributed/amplab/compact/q2.k3 tools/ktrace/yaml/amplab.yaml tools/ktrace/queries/amplab_q2/correct.csv
#./tools/ktrace/run_test.sh examples/distributed/amplab/compact/q3.k3 tools/ktrace/yaml/amplab.yaml tools/ktrace/queries/amplab_q3/correct.csv
