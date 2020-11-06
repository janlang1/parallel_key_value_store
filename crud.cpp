#include "crud.hpp"
#include "Protocol.hpp"

void Crud::Create(std::string k, std::string v) {
    hashtable.emplace(k, v);
}

std::string Crud::Read(std::string k) {
    return (hashtable.find(k) != hashtable.end()) ? hashtable[k] : FAIL;
}

void Crud::Update(std::string k, std::string v) {
    if (hashtable.find(k) != hashtable.end()) {
        hashtable[k] = v;
    }
}

void Crud::Delete(std::string k) {
    auto it = hashtable.find(k);
    if (it != hashtable.end()) {
        hashtable.erase(hashtable.find(k));
    }
}
