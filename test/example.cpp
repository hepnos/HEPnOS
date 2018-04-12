#include <iostream>
#include <hepnos/DataStore.hpp>
#include <hepnos/DataSet.hpp>

using namespace hepnos;

int main(int argc, char** argv) {

    DataStore datastore(argv[1]);
    std::cout << "====== Testing createDataSet =======" << std::endl;
    DataSet dataset1 = datastore.createDataSet("AAAA");
    std::cout << "Created " << dataset1.fullname() << std::endl;
    DataSet dataset2 = datastore.createDataSet("BBB");
    std::cout << "Created " << dataset2.fullname() << std::endl;
    DataSet dataset3 = datastore.createDataSet("sssd");
    std::cout << "Created " << dataset3.fullname() << std::endl;
    DataSet dataset4 = datastore.createDataSet("myproject");
    std::cout << "Created " << dataset4.fullname() << std::endl;
    std::cout << "====== Testing next ================" << std::endl;
    DataSet n = dataset4.next();
    std::cout << "Next is " << n.fullname() << std::endl;
    std::cout << "====== Testing find ================" << std::endl;
    for(auto it = datastore.find("BBB");
            it != datastore.end(); ++it) {
        std::cout << "Dataset from iterator: " << it->fullname() << std::endl;
    }
    std::cout << "====== Testing begin ===============" << std::endl;
    for(auto it = datastore.begin();
            it != datastore.end(); ++it) {
        std::cout << "Dataset from iterator: " << it->fullname() << std::endl;
    }
    std::cout << "====== Testing lower_bound =========" << std::endl;
    { 
        auto it1 = datastore.lower_bound("BBB");
        std::cout << "lower_bound(\"BBB\") = " << it1->fullname() << " (should be \"BBB\")" << std::endl;
        auto it2 = datastore.lower_bound("BAA");
        std::cout << "lower_bound(\"BAA\") = " << it2->fullname() << " (should be \"BBB\")" << std::endl;
    }
    std::cout << "====== Testing upper_bound =========" << std::endl;
    {
        auto it1 = datastore.upper_bound("myproject");
        std::cout << "upper_bound(\"myproject\") = " << it1->fullname() << " (should be \"sssd\")" << std::endl;
        auto it2 = datastore.upper_bound("myprojex");
        std::cout << "upper_bound(\"myprojex\") = " << it2->fullname() << " (should be \"sssd\")" << std::endl;
        auto it3 = datastore.upper_bound("myproj");
        std::cout << "upper_bound(\"myproject\") = " << it3->fullname() << " (should be \"myproject\")" << std::endl;
    }
    std::cout << "====== Testing store ===============" << std::endl;
    {
        std::string key("matthieu");
        std::string value("mdorier@anl.gov");
        dataset4.store(key, value);
    }
    std::cout << "====== Testing load ===============" << std::endl;
    {
        std::string key("matthieu");
        std::string value;
        dataset4.load(key, value);
        std::cout << "load(\"matthieu\") = " << value << std::endl;
    }
    std::cout << "====== Testing operator[] =========" << std::endl;
    std::cout << "datastore[\"myproject\"].fullname() = " << datastore["myproject"].fullname() << std::endl;
    return 0;
}
