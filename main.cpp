#include <cassert>
#include <iostream>
#include <string>

#include "ConsistentHasher.hpp"
#include "MultiHash.hpp"
#include "Protocol.hpp"

using namespace std;

int main()
{
    
    ConsistentHasher consistentHasher = ConsistentHasher(4);
    consistentHasher.addNode(1);
    consistentHasher.addNode(2);
    consistentHasher.addNode(3);
    consistentHasher.addNode(4);
    
    consistentHasher.printHashes();
    consistentHasher.printHashesToNodes();
    
    string s1 = "Jack";
    string s2 = "Jerry";
    string s3 = "Donald";
    cout << "Sending request for key " << s1 << " to " << consistentHasher.sendRequestTo(s1) << endl;
    cout << "Sending request for key " << s2 << " to " << consistentHasher.sendRequestTo(s2) << endl;
    cout << "Sending request for key " << s3 << " to " << consistentHasher.sendRequestTo(s3) << endl;
    
    
    assert(!formattedProperly(""));
    assert(!formattedProperly("C "));
    assert(formattedProperly("C Jack jackli2014@gmail.com"));
    
    assert(!formattedProperly("X Jack jackli2014@gmail.com"));
    assert(!formattedProperly("C Jack jackl i2014@gmail.com"));
    assert(!formattedProperly("C Jack"));
    assert(formattedProperly("R Jack"));
    assert(!formattedProperly("R Ja ck"));
     
    cout << "All tests passed" << endl;
     
    return 0;
}
     
