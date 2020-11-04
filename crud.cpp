#include<iostream>
#include<cstdlib>
#include<string>
#include<cstdio>
#include<unordered_map>

using namespace std;

const int T_S = 200;
//only INTS right now but will change to be templated with Key k Value v
class Crud {
   private:
      unordered_map<string,string> hashtable;
   public:
      Crud() {
          hashtable = unordered_map<string,string>();
      }
      
      void create(string k, string v) {
        hashtable.emplace(k, v);
      }

      string retrieve(string k) {
         return hashtable[k];
      }

      void Remove(string k) {
         hashtable.erase(hashtable.find(k)); 
      }

      ~Crud() {
      }
};
int main() {
   
   return 0;
}