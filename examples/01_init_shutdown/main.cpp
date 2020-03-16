#include <iostream>
#include <string>
#include <hepnos.hpp>

using namespace hepnos;

int main(int argc, char** argv) {
    
    if(argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <configfile>" << std::endl;
        exit(-1);
    }

    std::string configFile(argv[1]);

    DataStore datastore = DataStore::connect(configFile);

    // ...

    // only if you want to shutdown the HEPnOS service
    datastore.shutdown();
}
