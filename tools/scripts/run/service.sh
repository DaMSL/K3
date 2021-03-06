#! /bin/bash

# Command for compiling cpp file in k3
# Since it's a little tricky to come up with the full command line, you can just use this
# You might need to adjust it to suit your system

export K3HOME=/k3/K3
cd $K3HOME

export SNDPATH=`find .cabal-sandbox -name "*packages.conf.d"`

# Set a default c++ compiler
if [ -z "$CXX" ]; then
  CXX="--gcc"
fi

echo "K3HOME: $K3HOME"
echo "K3_CXXFLAGS: ${K3_CXXFLAGS}"

dist/build/k3/k3 \
  -I lib/k3 -I examples/sql \
  --mpargs package-db=$SNDPATH --mpsearch src \
  service $1 \
    $CXX -l cpp \
    --cpp-flags="-DK3MESSAGETRACE -DK3GLOBALTRACE -DK3TRIGGERTIMES -DBOOST_LOG_DYN_LINK -DYAS_SERIALIZE_BOOST_TYPES=1 -Iruntime/cpp/include/external -Iruntime/cpp -Iruntime/cpp/include ${K3_CXXFLAGS} -lboost_serialization -lboost_system -lboost_regex -lboost_thread -lyaml-cpp -lpthread -lboost_log_setup -lboost_log -lboost_program_options -lcsvpp -ldynamic -ftemplate-depth-1024 -O4" \
    -r runtime ${@:2} 
