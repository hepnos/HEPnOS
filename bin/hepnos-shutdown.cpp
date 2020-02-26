#include <iostream>
#include <hepnos.hpp>

int main(int argc, char* argv[])
{
    if(argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <connection.yaml>" << std::endl;
        exit(-1);
    }

    hepnos::DataStore datastore = hepnos::DataStore::connect(std::string(argv[1]));
    datastore.shutdown();

    return 0;
}
