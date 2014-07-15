# Command for compiling cpp file in k3
# Since it's a little tricky to come up with the full command line, you can just use this
# You might need to adjust it to suit your system
dist/build/k3/k3 -I ../K3-Core/lib/k3 compile -l cpp --cpp-flags='-DBOOST_LOG_DYN_LINK -I../K3-Core/runtime/cpp -lboost_serialization -lboost_system -lboost_regex -lboost_thread -lpthread -lboost_log_setup -lboost_log -lboost_program_options -O4' -r ../K3-Core/runtime $@
