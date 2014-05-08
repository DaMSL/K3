CC       = clang
CXX      = clang++
RM       = rm -f
CPPFLAGS = -I../
LDFLAGS  = 
LDLIBS   = 

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
	$(CXX) -o $@ $< $(LDFLAGS) $(LDLIBS)

%.o: %.cpp
	$(CXX) $(CPPFLAGS) -c -o $@ $<

clean:
    $(RM) $(OBJECTS) $(BINARIES)

