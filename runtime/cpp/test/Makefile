CC       = clang
CXX      = clang++
RM       = rm -f

K3IN = ../


CPPFLAGS = -std=c++11       \
	   -I$(K3IN)	        \
	   -I$(BOOSTIN)	        \
	   -I$(XUNITIN)         \
	   -DBOOST_LOG_DYN_LINK \
	   -D__stdcall=         \
	   -fPIC

LDFLAGS = -L$(BOOSTLIB) -L$(XUNITLIB)
LDLIBS  = -lboost_system            \
          -lboost_filesystem        \
          -lboost_regex             \
          -lboost_serialization     \
          -lboost_thread            \
          -lboost_chrono            \
          -lboost_log_setup         \
          -lboost_log               \
          -lxUnit++.Debug           \


SOURCES = IOTests.cpp           \
		  ListenerTest.cpp      \
		  FileDataspaceTest.cpp \
		  CollectionsTest.cpp   \
		  FibonacciTest.cpp     \
		  CountPeersTest.cpp

OBJECTS = $(SOURCES:.cpp=.o)
BINARES = $(OBJECTS:.o=.so)

include NonPortable.mak

all: $(OBJECTS) $(BINARIES)

%: %.o
	$(CXX) -shared -o $@ $< $(LDFLAGS) $(LDLIBS)

%.so: %.o
	$(CXX) -shared -o $@ $< $(LDFLAGS) $(LDLIBS)

%.o: %.cpp
	$(CXX) $(CPPFLAGS) -c -o $@ $<

.PHONY: clean
clean:
	$(RM) $(OBJECTS) $(BINARIES)

