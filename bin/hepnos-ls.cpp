#include <iostream>
#include <hepnos.hpp>

template<typename T>
void display_products(int level, T& item) {
    auto products = item.listProducts();
    for(auto& id : products) {
        for(int i=0; i < level; i++) std::cout << " ";
        std::string label, type;
        id.unpackInformation(nullptr, nullptr, nullptr, nullptr, &label, &type);
        std::cout << label << " -> " << type << std::endl;
    }
}

void navigate_subrun(int level, hepnos::SubRun& sr) {
    for(int i=0; i < level; i++) std::cout << " ";
    std::cout << "| [S] " << sr.number() << std::endl;
    display_products(level+1, sr);
    for(auto& e : sr) {
        for(int i=0; i < level+1; i++) std::cout << " ";
        std::cout << "| [E] " << e.number() << std::endl;
        display_products(level+2, e);
    }
}

void navigate_run(int level, hepnos::Run& r) {
    for(int i=0; i < level; i++) std::cout << " ";
    std::cout << "| [R] " << r.number() << std::endl;
    display_products(level+1, r);
    for(auto& sr : r) {
        navigate_subrun(level+1, sr);
    }
}

void navigate_dataset(int level, hepnos::DataSet& ds) {
    for(int i=0; i < level; i++) std::cout << " ";
    std::cout << "| [D] " << ds.name() << std::endl;
    display_products(level+1, ds);
    for(auto& sub_ds : ds) {
        navigate_dataset(level+1, sub_ds);
    }
    for(auto it = ds.runs().begin(); it != ds.runs().end(); it++) {
        navigate_run(level+1, *it);
    }
}

int main(int argc, char* argv[])
{
    if(argc != 3 ) {
        std::cerr << "Usage: " << argv[0] << " <protocol> <config.json>" << std::endl;
        exit(-1);
    }

    hepnos::DataStore datastore = hepnos::DataStore::connect(argv[1], argv[2]);
    for(auto& ds : datastore.root()) {
        navigate_dataset(0, ds);
    }

    return 0;
}
