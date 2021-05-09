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

    RunDescriptor runDescriptor;
    SubRunDescriptor subrunDescriptor;
    EventDescriptor eventDescriptor;

    // get the root of the DataStore
    DataSet root = datastore.root();
    // ----------------------------------------------------------------
    {
        // create a DataSet
        DataSet example10 = root.createDataSet("example10");
        std::cout << "Created dataset " << example10.fullname() << std::endl;
        // create a DataSet inside the example10 DataSet
        DataSet exp1 = example10.createDataSet("exp1");
        std::cout << "Create dataset " << exp1.fullname() << std::endl;
        // create a Run with number 36 inside the DataSet
        Run r = exp1.createRun(36);
        std::cout << "Created run " << r.number() << std::endl;
        // create a SubRun with number 42 inside this Run
        SubRun sr = r.createSubRun(42);
        std::cout << "Created subrun " << sr.number() << std::endl;
        // create an Event with number 13 inside this SubRun
        Event ev = sr.createEvent(13);
        std::cout << "Created event " << ev.number() << std::endl;
        // serialize the descriptors
        r.toDescriptor(runDescriptor);
        sr.toDescriptor(subrunDescriptor);
        ev.toDescriptor(eventDescriptor);
    }
    // ----------------------------------------------------------------
    {
        // accessing example10 dataset
        DataSet example10 = root["example10"];
        std::cerr << "DataSet retrieved: " << example10.name() << std::endl;
        // accessing exp1 dataset
        DataSet exp1 = example10["exp1"];
        std::cerr << "DataSet retrieved: " << exp1.name() << std::endl;
        // rebuilding run 36
        Run r = Run::fromDescriptor(datastore, runDescriptor);
        std::cerr << "Run retrieved: " << r.number() << std::endl;
        // rebuilding subrun 42
        SubRun sr = SubRun::fromDescriptor(datastore, subrunDescriptor);
        std::cerr << "Event retrieved: " << sr.number() << std::endl;
        // rebuilding event 13
        Event ev = Event::fromDescriptor(datastore, eventDescriptor);
        std::cerr << "Event retrieved: " << ev.number() << std::endl;
    }
}
