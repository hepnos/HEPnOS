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
    // Create a DataSet, a Run, a SubRun, and an Event
    DataSet example8 = root.createDataSet("example8");
    Run run = example8.createRun(1);
    SubRun subrun = run.createSubRun(4);
    Event event = subrun.createEvent(32);
    // Store a product into the event
    {
        Particle p("electron", 3.4, 4.5, 5.6);
        ProductID pid = event.store("mylabel", p);
    }
    // Reload a product from the event
    {
        Particle p;
        bool b = event.load("mylabel", p);
        if(b) std::cout << "Particle loaded succesfully" << std::endl;
        else  std::cout << "Particle wasn't loaded" << std::endl;
    }
    // Store a section of a vector into the event
    {
        std::vector<Particle> v;
        for(unsigned i=0; i < 5; i++) {
            v.emplace_back("electron", i*4, i*2, i+1);
        }
        // store only the sub-vector [1,3[ (2 elements)
        event.store("myvec", v, 1, 3);
    }
    // Load the vector
    {
        std::vector<Particle> v;
        event.load("myvec", v);
        std::cout << "Reloaded " << v.size() << " particles" << std::endl;
    }
}
