#include <netinet/in.h>
#include <pthread.h>
#include <cassert>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <time.h>
#include <iostream>
#include <iomanip>
#include <random>
#include <string>
#include <mpi.h>

#include "ConsistentHasher.hpp"
#include "Protocol.hpp"
#include "crud.hpp"
//#include "pthread_barrier_t.hpp"

#define ERROR 1
#define CTRLC 2
// #define N_WORKERS 2  // Number of worker nodes. This should always be set to 1 less than N in the script run_server.sh

using namespace std;

int exit_code = 0;
/*
pthread_mutex_t mux;
pthread_barrier_t barrier;
 */
ConsistentHasher consistentHasher;
//bool failed[N_WORKERS];

// Random number generators for failing nodes
/*
random_device rd;
mt19937 gen(rd());
uniform_real_distribution<> dis(0.0, 1.0);
 */

// Handle SIGINT signals
void signal_handler(int signal_number) {
    if (signal_number == SIGINT) {
        cout << "Terminating program" << endl;
        exit_code = CTRLC;
    }
}
/*
int numFailedNodes() {
    int n_failed_nodes = 0;
    for (int i = 0; i < N_WORKERS; i++) {
        if (failed[i]) {
            n_failed_nodes++;
        }
    }
    return n_failed_nodes;
}
*/
void *serveClient(void *arg) {
    int client_address_len, socket_fd, client_socket_fd;
    struct sockaddr_in client_address;
    
    /* Accept connection from client */
    client_address_len = sizeof(client_address);
    socket_fd = *((int *) arg);
    client_socket_fd = accept(socket_fd, (struct sockaddr *) &client_address, (socklen_t *) &client_address_len);
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
        //cout << "hello from master" << endl;
        /* Get client request */
        string request(MAX_STR_LEN, ' ');
        int n_bytes_read = read(client_socket_fd, (void *) request.c_str(), MAX_STR_LEN);
        if (n_bytes_read < 0) {
            cerr << "Error reading or timeout" << endl;
            break;
        }
                
        /* Shrink to appropriate size */
        // cout << "Read " << n_bytes_read << " from client" << endl;
        request.resize(n_bytes_read);
                
        /* Client has no more requests */
        if (request == DONE) {
            cout << "Client done" << endl;
            break;
        }
                
        /* Extract the key from the request and send it to the appropriate worker node */
        string key = getKey(request);
        int node = consistentHasher.sendRequestTo(key);
        
        /* Randomly fail a node if half the number of nodes haven't failed yet */
        /*
        double p_fail = dis(gen);
        cout << "p_fail is " << p_fail << endl;
        if (p_fail <= 0.2) {
            pthread_mutex_lock(&mux);
            int n_failed_nodes = numFailedNodes();
            if (!failed[node - 1] && (n_failed_nodes < N_WORKERS / 2) && N_WORKERS > 1) {
                failed[node - 1] = true;
                cout << "Node " << node << " failed!" << endl;
                consistentHasher.removeNode(node);
            }
            pthread_mutex_unlock(&mux);
        }
        */
        /* Wait for all failed nodes to be removed, if any */
        //pthread_barrier_wait(&barrier);
        
        /* If this node has failed, reroute it to another */
        /*
        if (failed[node - 1]) {
            node = consistentHasher.sendRequestTo(key);
        }
         */
        cout << "Sending request " << request << " to node " << node << endl;
        MPI_Send(request.c_str(), request.size(), MPI_CHAR, node, 0, MPI_COMM_WORLD);
                
        /* READ requests should return a value back from the worker node */
        if (request[0] == READ) {
            string value(MAX_STR_LEN, ' ');
            // cout << "Waiting for value for key " << key << endl;
            MPI_Recv((void *) value.c_str(), MAX_STR_LEN, MPI_CHAR, node, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            value = value.substr(0, value.find(" ", 0));
            // cout << "The value is " << value << endl;
            if (write(client_socket_fd, value.c_str(), value.size()) < 0) {
                cerr << "Error writing" << endl;
                break;
            }
        }
    }
    
    /* Reduce number of threads client has to wait for upon exiting */
    /*
    pthread_mutex_lock(&mux);
    pthread_barrier_update(&barrier);
    pthread_mutex_unlock(&mux);
     */
    return NULL;
}


int main(int argc, char** argv) {
    int size, rank, provided;
    int ret = MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    assert(ret == 0 && provided == MPI_THREAD_MULTIPLE);
    MPI_Comm_size(MPI_COMM_WORLD,&size);
    MPI_Comm_rank(MPI_COMM_WORLD,&rank);
    Crud cruds[size - 1];
   
    /* Master process */
    if (rank == 0) {
        /* Hash each worker node before accepting any client requests */
        cout << "There are " << (size - 1) << " worker nodes" << endl;
        consistentHasher.setN(size - 1);
        for (int i = 1; i < size; i++) {
            consistentHasher.addNode(i);
        }
        
        /* Initialize synchronization resources */
        /*
        pthread_barrier_init(&barrier, NULL, N_WORKERS);
        pthread_mutex_init(&mux, NULL);
         */
        
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
        if (::bind(socket_fd, (struct sockaddr *)&address, sizeof(address)) == -1) {
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
        signal(SIGINT, signal_handler);
        if (exit_code == CTRLC) {
            MPI_Bcast(&exit_code, 1, MPI_INT, 0, MPI_COMM_WORLD);
        }
        
        /* Start accepting client connections */
        if (!exit_code) {
            // Initialize fail array
            for (int i = 0; i < N_WORKERS; i++) {
                failed[i] = false;
            }
            
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
                if (pthread_join(pthreads[i], NULL)) {
                    cerr << "Error joining thread" << endl;
                    exit_code = ERROR;
                    MPI_Bcast(&exit_code, 1, MPI_INT, 0, MPI_COMM_WORLD);
                    break;
                }
            }
            
            cout << "Joined threads" << endl;
            /*
            pthread_barrier_destroy(&barrier);
            pthread_mutex_destroy(&mux);
             */
        }
    }
    else {
        if (!exit_code) {
            while (1) {
                // cout << "Hello from worker " << rank << endl;
                /* 1. Receive client request */
                int found;
                string request(MAX_STR_LEN, ' ');
                MPI_Recv((void *) request.c_str(), request.size(), MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                
                found = request.find_last_not_of(' '); // Remove trailing spaces
                request.erase(found + 1);
                /*
                cout << "Received request " << request << endl;
                cout << "Request has size " << request.size() << endl;
                */
                /* 2. Parse command. Determine what operation it is, separate key and value, and perform the operation using instance of CRUD class */
                //first letter
                string k = getKey(request);
                string v;
                double t1, t2;
                t1 = MPI_Wtime();
                
                if (request[0] == CREATE){
                    v = getValue(request);
                    cruds[rank - 1].Create(k,v);
                } else if (request[0] == READ){
                    v = cruds[rank - 1].Read(k);
                    MPI_Send(v.c_str(), v.size(), MPI_CHAR, 0, 0, MPI_COMM_WORLD);
                } else if (request[0] == UPDATE){
                    v = getValue(request);
                    cruds[rank - 1].Update(k,v);
                } else {
                    cruds[rank - 1].Delete(k);
                }
                
                t2 = MPI_Wtime();
                cout << fixed;
                cout << setprecision(6) << "Execution time is " << (t2 - t1) << " for node " << rank << endl;
                
            }
        }
    }
    
    MPI_Finalize();
    return exit_code;
}
