#Introduction

There are 4 SQL programs benchmarked @ [Amplab-Benchmark](https://amplab.cs.berkeley.edu/benchmark/). These have been rewritten in K3.

#K3-Implementations

##Scan-Query

Two implementations of Scan-Query have been written. `scanQuery` utilizes a single node in a network wherease `scanQuery_MM` implements on multiple nodes in a network.

###scanQuery.k3

To run `scanQuery.k3` —

    k3 interpret -b -p 127.0.0.1:40000:role=\"tableSource\":pageRankThreshold=15 /Users/kartikthapar/WorkCenter/Projects/K3/core/examples/sqlBenchmarking/scanQuery.k3

Modify `pageRankThreshold` in the command line above.

###scanQuery_MM.k3

To run the program, you first need to create a topology file with the following information —

- masterNodeAddress: address of the master node
- role: tableSource
- numberOfNodes: total number of nodes
- nodes: K3 collection of nodes
- pageRankThreshold: integer threshold value (pageRank > X)
- totalRows: total number of rows in the table
- ports: port descriptions

A sample topology file can be found in `topology_scanQuery_MM.csv`.

To generate the K3 command, execute the python script `run_k3_scanQuery_MM.py` as follows —

	python run_k3_scanQuery_MM.py -t topology_scanQuery_MM.csv