#! /bin/sh
# Command for compiling cpp file in k3
# Since it's a little tricky to come up with the full command line, you can just use this
# You might need to adjust it to suit your system
export SNDPATH=`find .cabal-sandbox -name "*packages.conf.d"`

# Set a default c++ compiler
if [ -z "$CXX" ]; then
  CXX="--gcc"
fi

echo "K3_CXXFLAGS: ${K3_CXXFLAGS}"

BSLINCLUDE="-I/usr/local/include/bsl -I/usr/local/include/bdl"

dist/build/k3/k3 \
  -I lib/k3 -I examples/sql -I examples/distributed/amplab/ \
  --mpargs package-db=$SNDPATH --mpsearch src $K3MPARGS \
  compile \
    $CXX -l cpp \
    --cpp-flags="-DK3MESSAGETRACE -DK3GLOBALTRACE -DK3TRIGGERTIMES -DBOOST_LOG_DYN_LINK -DYAS_SERIALIZE_BOOST_TYPES=1 -Iruntime/cpp/src/ -Iruntime/cpp/src/external ${BSLINCLUDE} ${K3_CXXFLAGS} -lboost_serialization -lboost_system -lboost_regex -lboost_thread -lyaml-cpp -lpthread -lboost_log_setup -lboost_log -lboost_program_options -lcsvpp -ldynamic -lbsl -lbdl -ftemplate-depth-1024 -O4" \
    -r runtime $@
