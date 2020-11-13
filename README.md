# parallel_key_value_store

## Compilation
To compile the code, run $ make. This will build all the other dependendant binaries. See Makefile for all other targets. To remove all executables, run $ make clean.

## Execution
To start up the server, run the bash script run_server.sh like $ ./run_server.sh. In that script, there is a variable N indicating the total number of nodes, including the master. Since we benchmarked our code in terms of the number of worker nodes, N would be 2, 3, 5, and 9, corresponding to the number of workers 1, 2, 4, and 8 respectively. For example, if testing with 4 worker nodes, N = 5. If installation instructions were followed correctly, the program "mpirun", which runs a compiled mpi program should be installed under /usr/local/bin on Mac OS (all available versions).

Once the server is started up, open up as many terminals as there are for number of workers, which is also the number of clients. E.g. for 4 workers, this would be 4 extra terminal windows, excluding the one for the server. Each of the included data text files contains 16 properly formatted CRUD commands. Divide these 16 commands among the number of clients evenly (e.g. 8 commands each for 2 clients and 4 commands each for 4 clients) in a fashion similar to block cyclic distribution. For 2 sets of 8 commands, input them in this order: 1. Command1 2. Command2 1. Command3 2. Command4, etc. On the server side, several debugging statements will be printed to provide information on latency per request and numbers useful for computing load balancing factor per round (e.g. with 2 clients, there are 8 rounds).

When all 16 commands have been processed, in each client, enter "DONE" to inform the server of shutting down. This will cause all the clients to cleanly exit and inform the server to join all threads. On the server's terminal, CTRL-C to terminate the server. 

For testing in cases of node failure, uncomment the sections in server.cpp wrapped around with "NODE FAILURE". Leaving these sections commented will not fail any nodes.

# installation
1. download most stable version of MPI that is compatible with your current GCC, GLIBCXX, and CXXABI
https://www.open-mpi.org/
2. once the tar or zip file is downloaded uncompress it and an install file should be in the folder, the main thing is in the folder directory in the terminal run "make install all"
3. ./configure --enable-mpi-thread-multiple
4. You should now be able to compile your MPI code locally just how it is on the USC server.

# platform used 
MacOS 10.15.7 (19H2)
2.7 GHz Dual-Core Intel Core i5


