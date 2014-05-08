CC       = clang
CXX      = clang++
RM       = rm -f

K3IN = ../

BOOSTIN = /usr/local/Cellar/boost/1.55.0/include/
BOOSTLIB = /usr/local/Cellar/boost/1.55.0/lib/

XUNITIN = /Users/joshwheeler/software/xunit/xUnit++/
XUNITLIB = /Users/joshwheeler/software/xunit/bin/xUnit++/

CPPFLAGS = -std=c++11	\
	   -I$(K3IN)	\
	   -I$(BOOSTIN)	\
	   -I$(XUNITIN) \
	   -DBOOST_LOG_DYN_LINK

LDFLAGS   = -L$(BOOSTLIB) -L$(XUNITLIB)
LDLIBS  = -lboost_system-mt		\
		-lboost_filesystem-mt		\
		-lboost_regex-mt		\
		-lboost_serialization-mt	\
		-lboost_thread-mt		\
		-lboost_chrono-mt		\
		-lboost_log_setup-mt		\
		-lboost_log-mt			\
		-lxUnit++.Debug

SOURCES = IOTests.cpp           \
		  ListenerTest.cpp      \
		  FileDataspaceTest.cpp \
		  CollectionsTest.cpp   \
		  FibonacciTest.cpp     \
		  CountPeersTest.cpp

OBJECTS = $(SOURCES:.cpp=.o)
BINARES = $(OBJECTS:.o=)

all: $(OBJECTS) $(BINARIES)

%: %.o
	$(CXX) -shared -o $@ $< $(LDFLAGS) $(LDLIBS)

%.o: %.cpp
	$(CXX) $(CPPFLAGS) -c -o $@ $<

clean:
	$(RM) $(OBJECTS) $(BINARIES)

