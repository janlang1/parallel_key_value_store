#include <string>
#include <signal.h>
#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>

#include "Protocol.hpp"

using namespace std;
   
int main(int argc, char **argv){
    int socket_fd, n_bytes_read, n_bytes_sent, n_bytes_recv;

    //Create Socket
    socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd == -1){
        cerr << "Socket cannot be created!" << endl;
        return 1;
    }

    struct sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(PORT);
    server_address.sin_addr.s_addr = inet_addr(SERVER_IP.c_str());
   
    // Bind address to socket
    if (connect(socket_fd, (struct sockaddr *) &server_address, sizeof(server_address)) == -1) {
        cerr << "Cannot connect to server." << endl;
        return 2;
    }
    
    // Continuously parse user CRUD requests and wait for server's reply, if it's a READ request
    while (1) {
        string request(MAX_STR_LEN, ' ');
        string reply(MAX_STR_LEN, ' ');
        
        // Get request from user
        cout << "Enter a request in one of the following formats: " << endl;
        cout << "1. C <Key> <Value>" << endl;
        cout << "2. R <Key>" << endl;
        cout << "3. U <Key> <Value>" << endl;
        cout << "4. D <Key>" << endl;
        cout << "5. DONE" << endl;
        
        n_bytes_read = read(STDIN_FILENO, (void *) request.c_str(), MAX_STR_LEN);
        if (n_bytes_read < 0) {
            cerr << "Error reading" << endl;
            continue;
        }
        
        if (n_bytes_read) {
            request.resize(n_bytes_read - 1); // Remove trailing \n
        }
        
        string key;
        if (request != DONE) {
            key = getKey(request);
            cout << "key is " << key << endl;
        }
        
        // Check format
        if (!formattedProperly(request)) {
            cout << "Improperly formatted request. Try again." << endl;
            continue;
        }
        
        // Send to server
        n_bytes_sent = write(socket_fd, (void *) request.c_str(), request.size());
        if (n_bytes_sent < 0) {
            cerr << "Error writing" << endl;
            continue;
        }
        if (request == DONE) {
            break;
        }
        
        // Wait for reply from server if it's a READ request
        if (request[0] == READ) {
            n_bytes_recv = read(socket_fd, (void *) reply.c_str(), MAX_STR_LEN);
            if (n_bytes_read < 0) {
                cerr << "Error reading" << endl;
                continue;
            }
            
            reply.resize(n_bytes_recv);
            
            if (reply != FAIL) {
                cout << "Value for key " << key << " is " << reply << endl;
            }
            else {
                cout << "No value found for key " << key << endl;
            }
        }
    }

    close(socket_fd); // close socket
    return 0;
}
