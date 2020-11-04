#include <iostream>
#include "Protocol.hpp"

/* Returns whether a string representing a CRUD request is formatted properly according to defined protocol */
bool formattedProperly(std::string request) {
    if (request == DONE) {
        return true;
    }
    size_t len = request.size();
    
    // Have at least one op and one arg
    if (len > 2) {
        // Start with a valid operator, followed by a space
        if (request[0] != CREATE && request[0] != READ && request[0] != UPDATE && request[0] != DELETE && request[1] != ' ') {
            return false;
        }
        
        // C and U ops must have two non-empty args (key and value)
        if (request[0] == CREATE || request[0] == UPDATE) {
            std::string args = request.substr(2);
            size_t space_pos = args.find(' ');
            
            // Key and value must be separated by space
            if (space_pos == std::string::npos) {
                return false;
            }
            
            std::string key = args.substr(0, space_pos);
            std::string value = args.substr(space_pos + 1);
            /*
            std::cout << "space_pos is " << space_pos << std::endl;
            std::cout << "args is " << args << std::endl;
            std::cout << "key is " << key << std::endl;
            std::cout << "value is " << value << std::endl;
            */
            // Key and string cannot contain spaces within themselves
            return key.find(' ') == std::string::npos and value.find(' ') == std::string::npos;
        }
        // R and D ops must have one arg without any intermediate spaces
        else {
            std::string key = request.substr(2);
            return key.find(' ') == std::string::npos;
        }
    }
    return false;
}

/* Extracts the key from a request. Assumes the request is formatted properly */
std::string getKey(std::string request) {
    if (request[0] == CREATE || request[0] == UPDATE) {
        std::string args = request.substr(2);
        size_t space_pos = args.find(' ');
        return args.substr(0, space_pos);
    }
    else {
        return request.substr(2);
    }
}

/* Extracts the value from a request. Assumes the request is formatted properly and it's a CREATE or UPDATE op */
std::string getValue(std::string request) {
    std::string args = request.substr(2);
    size_t space_pos = args.find(' ');
    return args.substr(space_pos + 1);
}
