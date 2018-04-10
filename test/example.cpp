#include <iostream>
#include <hepnos/DataStore.hpp>
#include <hepnos/DataSet.hpp>

using namespace hepnos;

int main(int argc, char** argv) {

    DataStore datastore(argv[1]);
    DataSet dataset = datastore.createDataSet("myproject");

    return 0;
}
