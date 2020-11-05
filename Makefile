CC = g++
CFLAGS = -std=c++17

default: server client

ConsistentHasher.o:
	$(CC) $(CFLAGS) -c ConsistentHasher.cpp

Protocol.o:
	$(CC) $(CFLAGS) -c Protocol.cpp

main: ConsistentHasher.o Protocol.o
	$(CC) $(CFLAGS) main.cpp -o main ConsistentHasher.o Protocol.o

server: ConsistentHasher.o Protocol.o crud.o
	mpic++ $(CFLAGS) server.cpp -lpthread -o server ConsistentHasher.o Protocol.o crud.o

client: Protocol.o
	$(CC) $(CFLAGS) client.cpp -o client Protocol.o

# crud.cpp
# -g -Wall crud.cpp -o crud


crud.o: 
	$(CC) $(CFLAGS) -c crud.cpp

hashtable: hashtable.cpp
	$(CC) $(CFLAGS) -g -Wall hashtable.cpp -o hashtable

clean: 
	rm -f *.o server client

# ConsistentHasher.o Protocol.o crud hashtable main