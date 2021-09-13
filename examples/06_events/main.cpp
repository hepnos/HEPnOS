#include <iostream>
#include <string>
#include <hepnos.hpp>

using namespace hepnos;

int main(int argc, char** argv) {

    if(argc != 3) {
        std::cerr << "Usage: " << argv[0] << "<protocol> <configfile>" << std::endl;
        exit(-1);
    }

    DataStore datastore = DataStore::connect(argv[1], argv[2]);
    // Get the root of the DataStore
    DataSet root = datastore.root();
    // Create a DataSet
    DataSet example6 = root.createDataSet("example6");
    // Create a Run 0
    Run run = example6.createRun(0);
    // Create a SubRun 13
    SubRun subrun = run.createSubRun(13);
    // Create 5 Events 42 ... 46
    for(unsigned i = 0; i < 5; i++) {
        subrun.createEvent(i+42);
    }
    // Iterate over the Events
    std::cout << "Events:" << std::endl;
    for(auto& event : subrun) {
        std::cout << event.number() << std::endl;
    }

    // access a Event by its number
    Event event43 = subrun[43];

    // find the Event 43
    SubRun::iterator it = subrun.find(43);
    std::cout << it->number() << std::endl;

    // lower_bound(43) will point to the Event 43
    SubRun::iterator lb = subrun.lower_bound(43);
    // upper_bound(43) will point to the Event 44
    SubRun::iterator ub = subrun.upper_bound(43);
}
