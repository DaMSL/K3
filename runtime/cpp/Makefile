CXX=g++
CXXFLAGS+=-std=c++14 -g -I. -DBOOST_ALL_DYN_LINK
LDFLAGS+=-std=c++14 -g -lboost_serialization -lboost_system -lboost_regex -lboost_thread -lpthread -lboost_log_setup -lboost_log -lboost_program_options -lhiredis

OBJECTS= resolve_test.o Resolver.o Endpoint.o Context.o Engine.o Codec.o IOHandle.o

all: resolver

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $*.cpp
resolver: $(OBJECTS)
	$(CXX) $^ $(LDFLAGS) -o resolver

.PHONY: clean
clean:
	rm -f *.o resolver
