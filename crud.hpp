#include <iostream>
#include <cstdlib>
#include <string>
#include <cstdio>
#include <unordered_map>

// hashtable = unordered_map<string,string>();
class Crud {
public:
    Crud() {};
    void Create(std::string k, std::string v);
    std::string Read(std::string k);
    void Update(std::string k, std::string v);
    void Delete(std::string k);
private:
    std::unordered_map<std::string,std::string> hashtable;
};
