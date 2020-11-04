#include <algorithm>
#include <cassert>
#include <iostream>

#include "ConsistentHasher.hpp"
#include "MultiHash.hpp"

/* Adds a node to the ring by hashing it the specified number of times. Sort the list of hashes every time after introducing a new node to enable binary search when determining what node should handle a particular request. */
void ConsistentHasher::addNode(int node) {
    auto hashes = hashN(node, m_n);
    for (auto hash : hashes) {
        m_hashes.push_back(hash);
        m_hashes_to_nodes[hash] = node;
    }
    sort(m_hashes.begin(), m_hashes.end());
}

void ConsistentHasher::printHashes() {
    for (auto hash : m_hashes) {
        std::cout << "Hash is " << hash << std::endl;
    }
    std::cout << "---------------------" << std::endl;
}

void ConsistentHasher::printHashesToNodes() {
    for (auto it : m_hashes_to_nodes) {
        std::cout << "Hash: " << it.first << " , node: " << it.second << std::endl;
    }
    std::cout << "---------------------" << std::endl;
}

/* TODO: Remove node from ring */
void ConsistentHasher::removeNode(int node) {
    
}

/* Returns the node to handle the request for this key. */
int ConsistentHasher::sendRequestTo(std::string key) {
    auto hashes = hashN(key, 1);
    unsigned int key_hash = hashes[0];
    std::cout << "Hash for key " << key << " is " << key_hash << std::endl;
    
    /* Binary search to determine first node hash greater than the key's */
    int low = 0, high = m_hashes.size();
    while (low != high && low < m_hashes.size()) {
        int mid = (low + high) / 2;
        if (m_hashes[mid] <= key_hash) {
            low = mid + 1;
        }
        else {
            high = mid;
        }
    }
    
    if (low < m_hashes.size()) { // Hash of key is not greater than the greatest hash of nodes
        return m_hashes_to_nodes[m_hashes[high]];
    }
    else { // Wrap-around
        return m_hashes_to_nodes[m_hashes[0]];
    }
}
