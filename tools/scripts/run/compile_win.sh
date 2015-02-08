# Command for compiling cpp file in k3
# Since it's a little tricky to come up with the full command line, you can just use this
# You might need to adjust it to suit your system
export SNDPATH=`find .cabal-sandbox -name "*packages.conf.d"`
dist/build/k3/k3 \
  -I ../K3-Core/lib/k3 -I ../K3-Core/examples/sql \
  --mpargs package-db=$SNDPATH --mpsearch ../K3-Core/src \
  compile \
    --gcc -l cpp \
    --cpp-flags='-DBOOST_LOG_DYN_LINK -I../K3-Core/runtime/cpp/external -I../K3-Core/runtime/cpp -L/mingw64/lib -L../extra/lib -lboost_serialization-mt -lboost_system-mt -lboost_regex-mt -lboost_thread-mt -lre2 -lyaml-cpp -lpthread -lboost_chrono-mt -lboost_log_setup-mt -lboost_log-mt -lboost_program_options-mt -lwsock32 -lws2_32 -ftemplate-depth-1024 -O4' \
    -r ../K3-Core/runtime $@
