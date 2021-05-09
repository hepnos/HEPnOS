#include <iostream>
#include <string>
#include <hepnos.hpp>

using namespace hepnos;

int main(int argc, char** argv) {

    if(argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <protocol> <configfile>" << std::endl;
        exit(-1);
    }

    DataStore datastore = DataStore::connect(argv[1], argv[2]);
    // Get the root of the DataStore
    DataSet root = datastore.root();
    // Create a DataSet
    DataSet example4 = root.createDataSet("example4");
    // Create 5 Runs 42 ... 46
    for(unsigned i = 0; i < 5; i++) {
        example4.createRun(i+42);
    }
    // Iterate over the Runs
    std::cout << "Runs:" << std::endl;
    for(auto& run : example4.runs()) {
        std::cout << run.number() << std::endl;
    }

    // access a Run by its number
    Run run43 = example4[43];

    // find the Run 43
    RunSet::iterator it = example4.runs().find(43);
    std::cout << it->number() << std::endl;

    // lower_bound(43) will point to the Run 43
    RunSet::iterator lb = example4.runs().lower_bound(43);
    // upper_bound(43) will point to the Run 44
    RunSet::iterator ub = example4.runs().upper_bound(43);
}
