# Run

To run the theteJoin program, use the following command line statement:

    cabal run -- k3 -I ../core/lib/ interpret -b -p 127.0.0.1:40000:role=\"master\" -p 127.0.0.1:4010 -p 127.0.0.1:4020 -p 127.0.0.1:4030 -p 127.0.0.1:4040 -p 127.0.0.1:5010 -p 127.0.0.1:5020 -p 127.0.0.1:5030 -p 127.0.0.1:5040 -p 127.0.0.1:6010 -p 127.0.0.1:6020 -p 127.0.0.1:6030 -p 127.0.0.1:6040 -p 127.0.0.1:6050 -p 127.0.0.1:6060 -p 127.0.0.1:6070 -p 127.0.0.1:6080 -p 127.0.0.1:6090 ../core/examples/distributed/MapReduce/ThetaJoin/thetaJoin.k3

Peers with ports 6xxx are used as reducers (9) whereas peers with ports 4xxx (4) and 5xxx (4) are used as mappers.
