CXX=clang++
CXXFLAGS+=-std=c++14 -g -I. -DBOOST_ALL_DYN_LINK
LDFLAGS+=-g -lboost_serialization -lboost_system -lboost_regex -lboost_thread -lpthread -lboost_log_setup -lboost_log -lboost_program_options -lhiredis

OBJECTS= Resolver.o resolve_test.o Endpoint.o Context.o

all: resolver

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $*.cpp
resolver: $(OBJECTS)
	$(CXX) $^ $(LDFLAGS) -o resolver

.PHONY: clean
clean:
	rm -f *.o resolver
