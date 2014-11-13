# Command for compiling cpp file in k3 on OSX with homebrew
export SNDPATH=`find .cabal-sandbox -name "*packages.conf.d"`
dist/build/k3/k3 -I ../K3-Core/lib/k3 -I ../K3-Core/examples/sql compile --gcc -l cpp --cpp-flags='-DBOOST_LOG_DYN_LINK -I../K3-Core/runtime/cpp/external -I../K3-Core/runtime/cpp -lyaml-cpp -lboost_serialization-mt -lboost_system-mt -lboost_regex-mt -lboost_thread-mt -lre2 -lpthread -lboost_log_setup-mt -lboost_log-mt -lboost_program_options-mt -ftemplate-depth-1024 -O4' -r ../K3-Core/runtime $@ --mpargs package-db=$SNDPATH --mpsearch ../K3-Core/src
