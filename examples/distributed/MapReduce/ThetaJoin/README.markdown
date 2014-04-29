# Run

## Run_Scripts & Topology

To run the thetaJoin program, first generate the K3 program using a topology file. Sample topology is given in `./topology_thetaJoin.csv`. Running the K3 program generator python script requires python module — `docopt`. Use `easy_install` or `pip` to install docopt.

    python run_theta_join_k3.py --simulation --topology topology_thetaJoin.csv

Run the generated program code in `.../K3/driver/`. Prefix with `cabal run -- k3 -I ../core/lib/` (if not aliased already).

Following options are configurable:

- masterNodeAddress: The node address at which the master node runs.
- role: The role for the master node.
- sMappers: Collection of type {Address : address, Id : int} for a list of S table mappers.
- tMappers: Collection of type {Address : address, Id : int} for a list of T table mappers.
- reducers: Collection of type {Address : address, Id : int} for a list of reducers.
- ports: All the peers (currently this option exists as the script does not parse the mapper/reducer collections; in future this will go!).
- maxS: Region height (No defaults).
- maxT: Region width (No defaults).

## Old Version (without topology)

To run previous versions without any topology file (see previous commits), use:

    cabal run -- k3 -I ../core/lib/ interpret -b -p 127.0.0.1:40000:role=\"master\" -p 127.0.0.1:4010 -p 127.0.0.1:4020 -p 127.0.0.1:4030 -p 127.0.0.1:4040 -p 127.0.0.1:5010 -p 127.0.0.1:5020 -p 127.0.0.1:5030 -p 127.0.0.1:5040 -p 127.0.0.1:6010 -p 127.0.0.1:6020 -p 127.0.0.1:6030 -p 127.0.0.1:6040 -p 127.0.0.1:6050 -p 127.0.0.1:6060 -p 127.0.0.1:6070 -p 127.0.0.1:6080 -p 127.0.0.1:6090 ../core/examples/distributed/MapReduce/ThetaJoin/thetaJoin.k3

Peers with ports 6xxx are used as reducers (9) whereas peers with ports 4xxx (4) and 5xxx (4) are used as mappers.
