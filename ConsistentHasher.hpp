#ifndef ConsistentHasher_hpp
#define ConsistentHasher_hpp

#include <string>
#include <vector>
#include <unordered_map>

/* Ring implementation of a consistent hashing scheme */
class ConsistentHasher
{
public:
    ConsistentHasher() { m_n = 1; };
    ConsistentHasher(int n): m_n(n) {};
    void printHashes();
    void printHashesToNodes();
    void setN(int n) { m_n = n; };
    void addNode(int node);
    void removeNode(int node);
    int sendRequestTo(std::string key);
private:
    int m_n; // # of times each node should be hashed
    std::vector<unsigned int> m_hashes; // ring of hashes for each node
    std::unordered_map<unsigned int, int> m_hashes_to_nodes; // mapping of hashes to their corresponding nodes
};

#endif
