export SNDPATH=`find ../.cabal-sandbox -name "*packages.conf.d"`
../dist/build/k3/k3 -I ../../K3-Core/lib/k3 -I ../../K3-Core/examples/sql compile -l ktrace $@  --mpargs package-db=$SNDPATH --mpsearch ../../K3-Core/src 
