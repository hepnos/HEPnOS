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
    DataSet example5 = root.createDataSet("example5");
    // Create a Run 0
    Run run = example5.createRun(0);
    // Create 5 SubRuns 42 ... 46
    for(unsigned i = 0; i < 5; i++) {
        run.createSubRun(i+42);
    }
    // Iterate over the SubRuns
    std::cout << "SubRuns:" << std::endl;
    for(auto& subrun : run) {
        std::cout << subrun.number() << std::endl;
    }

    // access a SubRun by its number
    SubRun subrun43 = run[43];

    // find the SubRun 43
    Run::iterator it = run.find(43);
    std::cout << it->number() << std::endl;

    // lower_bound(43) will point to the SubRun 43
    Run::iterator lb = run.lower_bound(43);
    // upper_bound(43) will point to the SubRun 44
    Run::iterator ub = run.upper_bound(43);
}
