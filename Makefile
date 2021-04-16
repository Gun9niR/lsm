
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall

all: correctness persistence

correctness:  SSTable.o kvstore.o correctness.o

persistence:  SSTable.o kvstore.o persistence.o

clean:
	-rm -f correctness persistence *.o
