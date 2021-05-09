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
    DataSet example9 = root.createDataSet("example9");
    Run run = example9.createRun(1);
    SubRun subrun = run.createSubRun(4);
    Event event = subrun.createEvent(32);
    // Store a product into the event
    {
        Particle p("electron", 3.4, 4.5, 5.6);
        ProductID pid = event.store("mylabel", p);
        Ptr<Particle> ptr = datastore.makePtr<Particle>(pid);
        event.store("myptr", ptr);
    }
    // Reload a product from the event
    {
        Particle p;
        Ptr<Particle> ptr;
        bool b = event.load("myptr", ptr);
        if(b) {
            std::cout << "Pointer loaded succesfully" << std::endl;
            p = *ptr;
            std::cout << "Particle pointed : "
                      << p.name << " "
                      << p.x << ", " << p.y << ", " << p.z << std::endl;
        } else {
            std::cout << "Pointer wasn't loaded" << std::endl;
        }
    }
    // Store a vector into the event and a pointer to an element
    {
        std::vector<Particle> v;
        for(unsigned i=0; i < 5; i++) {
            v.emplace_back("electron", i*4, i*2, i+1);
        }
        auto pid = event.store("myvec", v);
        // pointer to particle at index 3
        Ptr<Particle> ptr = datastore.makePtr<Particle>(pid,3);
        event.store("myptr2vec", ptr);
    }
    // Load the particle from its underlying vector
    {
        Ptr<Particle> ptr;
        event.load("myptr2vec", ptr);
        Particle p = *ptr;
    }
}
