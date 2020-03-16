#include <iostream>
#include <string>
#include <hepnos.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>

using namespace hepnos;

struct Particle {

    std::string name;
    double x, y, z;

    Particle() {}

    Particle(const std::string& name, double x, double y, double z)
    : name(name), x(x), y(y), z(z) {}

    template<typename Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & name;
        ar & x;
        ar & y;
        ar & z;
    }
};

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
    DataSet example13 = root.createDataSet("example13");
    {
        AsyncEngine async(datastore,1);
        // Create a Run, a SubRun, and many Events
        Run run = example13.createRun(async, 1);
        SubRun subrun = run.createSubRun(async, 4);
        for(unsigned i = 0; i < 20; i++) {
            Event event = subrun.createEvent(async, 32+i);
            // Store a product into the event
            Particle p("electron", 3.4+i, 4.5+i, 5.6+i);
            ProductID pid = event.store(async, "mylabel", p);
        }
    }
    // Reload using a Prefetcher and AsyncEngine
    {
        Run run = example13[1];
        SubRun subrun = run[4];

        AsyncEngine async(datastore, 1);
        Prefetcher prefetcher(async);
        // Enable loading Particle objects associated with the label "mylabel"
        prefetcher.fetchProduct<Particle>("mylabel");
        // Loop over the events in the SubRun using the Prefetcher
        for(auto& event : prefetcher(subrun)) {
            Particle p;
            bool b = event.load(prefetcher, "mylabel", p);
            if(b) std::cout << "Particle loaded succesfully" << std::endl;
            else  std::cout << "Particle wasn't loaded" << std::endl;
        }
    }
}
