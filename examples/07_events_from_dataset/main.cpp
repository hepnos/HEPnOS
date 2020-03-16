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
    DataSet example7 = root.createDataSet("example7");
    // Create 5 Runs with 5 SubRuns with 5 Events
    for(unsigned i=0; i < 5; i++) {
        auto run = example7.createRun(i);
        for(unsigned j=0; j < 5; j++) {
            auto subrun = run.createSubRun(j);
            for(unsigned k=0; k < 5; k++) {
                auto event = subrun.createEvent(k);
            }
        }
    }
    // Iterate over the events directly from the example7 DataSet
    for(auto& event : example7.events()) {
        SubRun subrun = event.subrun();
        Run run = subrun.run();
        std::cout << "Run " << run.number() 
                  << ", SubRun " << subrun.number()
                  << ", Event " << event.number()
                  << std::endl;
    }
    // Iterate target by target
    unsigned numTargets = datastore.numTargets(ItemType::EVENT);
    for(unsigned target = 0; target < numTargets; target++) {
        for(auto& event : example7.events()) {
            SubRun subrun = event.subrun();
            Run run = subrun.run();
            std::cout << "Run " << run.number() 
                      << ", SubRun " << subrun.number()
                      << ", Event " << event.number()
                      << std::endl;
        }
    }
}
