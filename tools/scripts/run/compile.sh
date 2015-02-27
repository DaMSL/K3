# Command for compiling cpp file in k3
# Since it's a little tricky to come up with the full command line, you can just use this
# You might need to adjust it to suit your system
export SNDPATH=`find .cabal-sandbox -name "*packages.conf.d"`
dist/build/k3/k3 \
  -I lib/k3 -I examples/sql \
  --mpargs package-db=$SNDPATH --mpsearch src \
  compile \
    --gcc -l cpp \
    --cpp-flags='-DBOOST_LOG_DYN_LINK -Iruntime/cpp/external -Iruntime/cpp -lboost_serialization -lboost_system -lboost_regex -lboost_thread -lre2 -lyaml-cpp -lpthread -lboost_log_setup -lboost_log -lboost_program_options -ftemplate-depth-1024 -O4' \
    -r runtime $@