#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <string>
#include <mpi.h>

#include "ConsistentHasher.hpp"
#include "Protocol.hpp"

#define ERROR 1
#define CTRLC 2

using namespace std;

void signal_handler(int signal_number); // Handle SIGINT signals

/*
typedef struct pthread_arg_t {
    int client_socket_fd;
    struct sockaddr_in client_address;
} pthread_arg_t;
*/
ConsistentHasher consistentHasher;

void *serveClient(void *arg) {
    int client_address_len, socket_fd, client_socket_fd;
    struct sockaddr_in client_address;
    
    /* Accept connection from client */
    client_address_len = sizeof(client_address);
    socket_fd = *((int *) arg);
    client_socket_fd = accept(socket_fd, (struct sockaddr *)&client_address, &client_address_len);
    if (client_socket_fd == -1) {
        cerr << "Error accepting" << endl;
        return NULL;
    }
    
    /* Timeout clients who are not responsive */
    struct timeval tv;
    tv.tv_sec = TIME_OUT;
    tv.tv_usec = 0;
    setsockopt(client_socket_fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));
    
    while (1) {
        /* Get client request */
        string request(MAX_STR_LEN);
        int n_bytes_read = read(client_socket_fd, request.c_str(), MAX_STR_LEN);
        if (n_bytes_read < 0) {
            cerr << "Error reading or timeout" << endl;
            break;
        }
        
        /* Client has no more requests */
        if (request == DONE) {
            break;
        }
        
        /* Shrink to appropriate size */
        request.resize(n_bytes_read);
        
        /* Extract the key from the request and send it to the appropriate worker node */
        string key = getKey(request);
        int node = consistentHasher.sendRequestTo(key);
        MPI_Send(request.c_str(), request.size(), MPI_CHAR, node, 0, MPI_COMM_WORLD);
        
        /* READ requests should return a value back from the worker node */
        if (request[0] == READ) {
            string value(MAX_STR_LEN, ' ');
            MPI_Recv(value.c_str(), request.size(), MPI_CHAR, node, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            value = value.substr(0, string1.find(" ", 0));
            if (write(client_socket_fd, value.c_str(), value.size()) < 0) {
                cerr << "Error writing" << endl;
                break;
            }
        }
    }
    
    return NULL;
}


int main(int argc, char** argv) {
    int exit_code, size, rank, provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_size(MPI_COMM_WORLD,&size);
    MPI_Comm_rank(MPI_COMM_WORLD,&rank);
    exit_code = 0;
   
    /* Master process */
    if (rank == 0) {
        /* Hash each worker node before accepting any client requests */
        consistentHasher.setN(size - 1);
        for (int i = 1; i < size; i++) {
            consistentHasher.addNode(i);
        }
        
        /* Set up TCP connections */
        int socket_fd, client_socket_fd;
        /*
        pthread_arg_t *pthread_arg;
        pthread_t pthread;
         */
        pthread_t *pthreads;
        struct sockaddr_in address;
        socklen_t client_address_len;
        
        pthreads = (pthread_t *) malloc(sizeof(pthread_t) * (size - 1));
        memset(&address, 0, sizeof address);
        address.sin_family = AF_INET;
        address.sin_port = htons(PORT);
        address.sin_addr.s_addr = INADDR_ANY;
        
        // Create TCP socket
        if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
            cerr << "Error creating socket" << endl;
            exit_code = ERROR;
            MPI_Bcast(&exit_code, 1, MPI_INT, 0, MPI_COMM_WORLD);
        }
        
        // Bind address to socket
        if (bind(socket_fd, (struct sockaddr *)&address, sizeof address) == -1) {
            cerr << "Error binding socket" << endl;
            exit_code = ERROR;
            MPI_Bcast(&exit_code, 1, MPI_INT, 0, MPI_COMM_WORLD);
        }
        
        // Listen on socket for up to the # of worker nodes
        if (listen(socket_fd, size - 1) == -1) {
            cerr << "Error listening" << endl;
            exit_code = ERROR;
            MPI_Bcast(&exit_code, 1, MPI_INT, 0, MPI_COMM_WORLD);
        }
        
        // Install signal handler (testing purposes)
        if (signal(SIGINT, signal_handler) == SIGINT) {
            cout << "Terminating program" << endl;
            exit_code = CTRLC;
            MPI_Bcast(&exit_code, 1, MPI_INT, 0, MPI_COMM_WORLD);
        }
        
        /* Start accepting client connections */
        if (!exit_code) {
            // Spawn a thread for each client
            for (int i = 0; i < size - 1; i++) {
                if (pthread_create(&pthreads[i], NULL, serveClient, (void *) &socket_fd)) {
                    cerr << "Error creating thread" << endl;
                    exit_code = ERROR;
                    MPI_Bcast(&exit_code, 1, MPI_INT, 0, MPI_COMM_WORLD);
                    break;
                }
            }
            
            // Join threads
            for (int i = 0; i < size - 1; i++) {
                if (pthread_join(&pthreads[i], NULL)) {
                    cerr << "Error joining thread" << endl;
                    exit_code = ERROR;
                    MPI_Bcast(&exit_code, 1, MPI_INT, 0, MPI_COMM_WORLD);
                    break;
                }
            }
        }
    }
    else {
        if (!exit_code) {
            /* 1. Receive client request */
            string request(MAX_STR_LEN);
            int status = 0;
            MPI_Recv(&request, request.size(), MPI_DOUBLE, rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            /* 2. Instantiate instance of CRUD class */
            Crud crud = new Crud();
            /* 3. Parse command. Determine what operation it is, separate key and value, and perform the operation using instance of CRUD class */
            //first letter 
            if(request[0] == 'C'){

            } else if (request[0] == 'R'){
                String value = 
                MPI_Send(&value, value.size(), MPI_DOUBLE, 1, 0, MPI_COMM_WORLD);

            } else if (request[0] == 'U'){
                
            } else if (request[0] == 'D'){
                
            }
            /* 4. If it's a READ op, send back the corresponding value for the key to P0 else send back a FAILURE Message (see Protocol.hpp) */
            //send to process 0 

            
        }
    }
    
    MPI_Finalize();
    return exit_code;
}
