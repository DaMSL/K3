#! /bin/sh
# Command for compiling cpp file in k3
# Since it's a little tricky to come up with the full command line, you can just use this
# You might need to adjust it to suit your system
module load boost/1.5.8
module load cuda/7.0
export SNDPATH=`find .cabal-sandbox -name "*packages.conf.d"`

# Set a default c++ compiler
if [ -z "$CXX" ]; then
  CXX="--gcc"
fi

echo "K3_CXXFLAGS: ${K3_CXXFLAGS}"

THRUSTINCLUDE="-I/cm/shared/apps/cuda/7.0/include/"
BSLINCLUDE="-I/usr/local/include/bsl -I/usr/local/include/bdl"
BOOSTINCLUDE="-I/cm/shared/apps/boost/1.5.8/include/ -I$HOME/include"

dist/build/k3/k3 \
  -I lib/k3 -I examples/sql -I examples/distributed/amplab/compact \
  --mpargs package-db=$SNDPATH --mpsearch src \
  compile \
    $CXX -l cpp \
    --cpp-flags="-DK3DEBUG -DBOOST_LOG_DYN_LINK -DYAS_SERIALIZE_BOOST_TYPES=1 -Iruntime/cpp/include/ -Iruntime/cpp/include/external ${BOOSTINCLUDE} ${BSLINCLUDE} ${THRUSTINCLUDE} ${K3_CXXFLAGS} -lboost_serialization -lboost_system -lboost_regex -lboost_thread -lyaml-cpp -lpthread -lboost_log_setup -lboost_log -lboost_program_options -lcsvpp -lcuda -O4 -L/cm/shared/apps/boost/1.5.8/lib -L$HOME/lib" \
    -r runtime $@
