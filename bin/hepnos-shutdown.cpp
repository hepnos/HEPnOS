#include <iostream>
#include <hepnos.hpp>

int main(int argc, char* argv[])
{
    if(argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <connection.yaml>" << std::endl;
        exit(-1);
    }

    hepnos::DataStore datastore(argv[1]);
    datastore.shutdown();

    return 0;
}
