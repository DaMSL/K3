# Command for compiling cpp file in k3 on OSX with homebrew
export SNDPATH=`find .cabal-sandbox -name "*packages.conf.d"`
dist/build/k3/k3 \
	-I lib/k3 -I examples/sql \
	--mpargs package-db=$SNDPATH --mpsearch src \
	compile \
	  --gcc -l cpp \
		--cpp-flags='-DYAS_SERIALIZE_BOOST_TYPES=1 -DBOOST_LOG_DYN_LINK -Iruntime/cpp/external -Iruntime/cpp -lboost_serialization-mt -lboost_system-mt -lboost_regex-mt -lboost_thread-mt -lyaml-cpp -lpthread -lboost_log_setup-mt -lboost_log-mt -lboost_program_options-mt -lcsvpp -ftemplate-depth-1024 -O4' \
		-r runtime $@
