CC = g++
CFLAGS = -std=c++17

default: main 

ConsistentHasher.o:
	$(CC) $(CFLAGS) -c ConsistentHasher.cpp

Protocol.o:
	$(CC) $(CFLAGS) -c Protocol.cpp

main: ConsistentHasher.o Protocol.o
	$(CC) $(CFLAGS) main.cpp -o main ConsistentHasher.o Protocol.o

crud: crud.cpp
	$(CC) $(CFLAGS) -g -Wall crud.cpp -o crud

hashtable: hashtable.cpp
	$(CC) $(CFLAGS) -g -Wall hashtable.cpp -o hashtable

clean: 
	rm -f crud hashtable main ConsistentHasher.o Protocol.o *.o