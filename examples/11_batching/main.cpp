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

    if(argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <protocol> <configfile>" << std::endl;
        exit(-1);
    }

    DataStore datastore = DataStore::connect(argv[1], argv[2]);
    // Get the root of the DataStore
    DataSet root = datastore.root();
    // Create a DataSet
    DataSet example11 = root.createDataSet("example11");
    // Create a Run, a SubRun, and an Event, but delay
    // the actual creation using a WriteBatch
    {
        WriteBatch batch(datastore);
        Run run = example11.createRun(batch, 1);
        SubRun subrun = run.createSubRun(batch, 4);
        Event event = subrun.createEvent(batch, 32);
        // Store a product into the event
        Particle p("electron", 3.4, 4.5, 5.6);
        ProductID pid = event.store(batch, "mylabel", p);
        // The batch is flushed at the end of the scope
    }
    // Reload a product from the event
    {
        auto run = example11[1];
        auto subrun = run[4];
        auto event = subrun[32];
        Particle p;
        bool b = event.load("mylabel", p);
        if(b) std::cout << "Particle loaded succesfully" << std::endl;
        else  std::cout << "Particle wasn't loaded" << std::endl;
    }
}
