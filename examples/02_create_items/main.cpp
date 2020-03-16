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

    // get the root of the DataStore
    DataSet root = datastore.root();
    // ----------------------------------------------------------------
    {
        // create a DataSet
        DataSet example2 = root.createDataSet("example2");
        std::cout << "Created dataset " << example2.fullname() << std::endl;
        // create a DataSet inside the example2 DataSet
        DataSet exp1 = example2.createDataSet("exp1");
        std::cout << "Create dataset " << exp1.fullname() << std::endl;
        // create a Run with number 36 inside the DataSet
        Run r = exp1.createRun(36);
        std::cout << "Created run " << r.number() << std::endl;
        // create a SubRun with number 42 inside this Run
        SubRun sr = r.createSubRun(42);
        std::cout << "Created subrun " << sr.number() << std::endl;
        // create an Event with number 13 inside this SubRun
        Event ev = sr.createEvent(13);
        std::cout << "Create event " << ev.number() << std::endl;
    }
    // ----------------------------------------------------------------
    {
        // accessing example2 dataset
        DataSet example2 = root["example2"];
        std::cerr << "DataSet retrieved: " << example2.name() << std::endl;
        // accessing exp1 dataset
        DataSet exp1 = example2["exp1"];
        std::cerr << "DataSet retrieved: " << exp1.name() << std::endl;
        // accessing run 36
        Run r = exp1[36];
        std::cerr << "Run retrieved: " << r.number() << std::endl;
        // accessing subrun 42
        SubRun sr = r[42];
        std::cerr << "Event retrieved: " << sr.number() << std::endl;
        // accessing event 13
        Event ev = sr[13];
        std::cerr << "Event retrieved: " << ev.number() << std::endl;
    }
}
