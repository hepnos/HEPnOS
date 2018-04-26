#include <iostream>
#include <hepnos/DataStore.hpp>
#include <hepnos/DataSet.hpp>
#include <hepnos/RunSet.hpp>
#include <hepnos/Run.hpp>

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

    std::cout << "====== Testing createRun ==========" << std::endl;
    Run run34 = dataset4.createRun(34);
    Run run43 = dataset4.createRun(43);
    Run run23 = dataset4.createRun(23);
    Run run56 = dataset4.createRun(56);
    std::cout << "created run with run number " << run34.number() << std::endl;
    std::cout << "created run with run number " << run43.number() << std::endl;
    std::cout << "created run with run number " << run23.number() << std::endl;
    std::cout << "created run with run number " << run56.number() << std::endl;
    
    std::cout << "====== Testing runs().begin() and runs().end() =====" << std::endl;
    for(auto& r : dataset4.runs()) {
        std::cout << "accessing a run with number " << r.number() << std::endl;
    }

    std::cout << "====== Testing runs().lower_bound() and upper_bound() ===" << std::endl;
    {
        auto it1 = dataset4.runs().lower_bound(34);
        auto it2 = dataset4.runs().lower_bound(33);
        auto it3 = dataset4.runs().lower_bound(35);
        std::cout << "lower_bound(34) = " << it1->number() << " (should be 34)" << std::endl;
        std::cout << "lower_bound(33) = " << it2->number() << " (should be 34)" << std::endl;
        std::cout << "lower_bound(35) = " << it3->number() << " (should be 43)" << std::endl;

        auto it4 = dataset4.runs().upper_bound(43);
        auto it5 = dataset4.runs().upper_bound(42);
        auto it6 = dataset4.runs().upper_bound(44);
        std::cout << "upper_bound(43) = " << it4->number() << " (should be 56)" << std::endl;
        std::cout << "upper_bound(42) = " << it5->number() << " (should be 43)" << std::endl;
        std::cout << "upper_bound(44) = " << it6->number() << " (should be 56)" << std::endl;
    }

    datastore.shutdown();

    return 0;
}
