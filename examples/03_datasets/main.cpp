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
    // Get the root of the DataStore
    DataSet root = datastore.root();
    // Create a DataSet
    DataSet example3 = root.createDataSet("example3");
    // Create 5 DataSets in example3
    for(unsigned i = 0; i < 5; i++) {
        std::string datasetName = "sub";
        datasetName += std::to_string(i+1);
        example3.createDataSet(datasetName);
    }
    // Iterate over the child datasets
    // This is equivalent to using begin() and end()
    std::cout << "Datasets in example3: " << std::endl;
    for(auto& dataset : example3) {
        std::cout << dataset.name() << std::endl;
    }

    // access a DataSet by its full name
    DataSet sub2 = root["example3/sub2"];

    // find the sub3 DataSet
    DataSet::iterator it = example3.find("sub3");
    std::cout << it->fullname() << std::endl;

    // lower_bound("sub3") will point to the sub3 dataset
    DataSet::iterator lb = example3.lower_bound("sub3");
    // upper_bound("sub3") will point to the sub4 dataset
    DataSet::iterator ub = example3.upper_bound("sub3");
}
