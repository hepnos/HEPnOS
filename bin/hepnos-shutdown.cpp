#include <iostream>
#include <hepnos.hpp>

int main(int argc, char* argv[])
{
    if(argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <config.json>" << std::endl;
        exit(-1);
    }

    hepnos::DataStore datastore =
        hepnos::DataStore::connect(argv[1], argv[2]);
    datastore.shutdown();

    return 0;
}
