#ifndef Protocol_hpp
#define Protocol_hpp

#include <string>

/* Protocol for our Key-Value store is defined as follows:
<OP> <Key> <Value>
 where OP is one of the CRUD operations listed below and Key and Value are both strings. For READ and DELETE, only a Key is needed (including a Value is not allowed.
 Each client will contact the server via localhost on the PORT defined below. The maximum allowed string length for both requests and replies for READ OPS is MAX_STR_LEN. The server will wait at most TIME_OUT seconds before closing the connection between an unresponsive client per request submission. To inform the server the client is done, the client will sent a special DONE request message. Then the server will terminate the connection and the client program can exit cleanly.
 */

#define PORT 8080
#define MAX_STR_LEN 256
#define TIME_OUT 300

/* CRUD operations */
const char CREATE = 'C';
const char READ = 'R';
const char UPDATE = 'U';
const char DELETE = 'D';

const std::string DONE = "DONE";

bool formattedProperly(std::string request);
std::string getKey(std::string request);
std::string getValue(std::string request);

#endif
