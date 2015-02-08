# Command for compiling cpp file in k3
# Since it's a little tricky to come up with the full command line, you can just use this
# You might need to adjust it to suit your system
export SNDPATH=`find .cabal-sandbox -name "*packages.conf.d"`
dist/build/k3/k3 \
  -I ../K3-Core/lib/k3 -I ../K3-Core/examples/sql -I ../K3-Core/examples/distributed/amplab \
  --mpargs package-db=$SNDPATH --mpsearch ../K3-Core/src \
  parse $@
