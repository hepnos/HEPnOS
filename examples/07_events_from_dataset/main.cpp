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
    DataSet example7 = root.createDataSet("example7");
    // Create 5 Runs with 5 SubRuns with 5 Events
    std::cout << "Creating Runs, SubRuns, and Events" << std::endl;
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
    std::cout << "Iterating over all Events" << std::endl;
    for(auto& event : example7.events()) {
        SubRun subrun = event.subrun();
        Run run = subrun.run();
        std::cout << "Run " << run.number() 
                  << ", SubRun " << subrun.number()
                  << ", Event " << event.number()
                  << std::endl;
    }
    // Iterate target by target
    std::cout << "Iterating over all Events target by target" << std::endl;
    unsigned numTargets = datastore.numTargets(ItemType::EVENT);
    for(unsigned target = 0; target < numTargets; target++) {
        std::cout << "Target " << target << std::endl;
        for(auto& event : example7.events(target)) {
            SubRun subrun = event.subrun();
            Run run = subrun.run();
            std::cout << "Run " << run.number() 
                      << ", SubRun " << subrun.number()
                      << ", Event " << event.number()
                      << std::endl;
        }
    }
}
