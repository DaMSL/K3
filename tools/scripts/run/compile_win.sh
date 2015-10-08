# Command for compiling cpp file in k3
# Since it's a little tricky to come up with the full command line, you can just use this
# You might need to adjust it to suit your system
export SNDPATH=`find .cabal-sandbox -name "*packages.conf.d"`
dist/build/k3/k3 \
  -I lib/k3 -I examples/sql \
  --mpargs package-db=$SNDPATH --mpsearch src \
  compile \
    --gcc -l cpp \
    --cpp-flags='-DK3DEBUG -DBOOST_LOG_DYN_LINK -DYAS_SERIALIZE_BOOST_TYPES=1 -D_WIN32_WINNT=0x600 -Iruntime/cpp/include -Iruntime/cpp/include/external -L/mingw64/lib -L../extra/lib -lboost_serialization-mt -lboost_system-mt -lboost_regex-mt -lboost_thread-mt -lyaml-cpp -lpthread -lboost_chrono-mt -lboost_log_setup-mt -lboost_log-mt -lboost_program_options-mt -lwsock32 -lws2_32 -lcsvpp -ldynamic -ftemplate-depth-1024 -O4' \
    -r runtime $@
