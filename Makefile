CXX = g++
CXXFLAGS = -g -O2 -pthread -std=c++0x
LDFLAGS += -lmesos -lpthread -lprotobuf
CXXCOMPILE = $(CXX) $(INCLUDES) $(CXXFLAGS) -c -o $@
CXXLINK = $(CXX) $(INCLUDES) $(CXXFLAGS) -o $@
HEADERS = constants.hpp

default: all
all: SomeSceduler MathExecutor

%: %.cpp $(HEADERS)
	$(CXXLINK) $< $(LDFLAGS)

clean:
	(rm -f SomeSceduler)
