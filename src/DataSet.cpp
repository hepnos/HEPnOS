#include "hepnos/DataSet.hpp"

namespace hepnos {

class DataSet::Impl {

    public:

        DataStore*   m_datastore;
        uint8_t      m_level;
        std::string  m_container;
        std::string  m_name;

        Impl(DataStore* ds, uint8_t level, const std::string& container, const std::string& name)
        : m_datastore(ds)
        , m_level(level)
        , m_container(container)
        , m_name(name) {}
};

DataSet::DataSet()
: m_impl(std::make_unique<DataSet::Impl>(nullptr, 0, "", "")) {}

DataSet::DataSet(DataStore& ds, uint8_t level, const std::string& fullname)
: m_impl(std::make_unique<DataSet::Impl>(&ds, level, "", "")) {
    size_t p = fullname.find_last_of('/');
    if(p == std::string::npos) {
        m_impl->m_name = fullname;
    } else {
        m_impl->m_name = fullname.substr(p+1);
        m_impl->m_container = fullname.substr(0, p);
    }
}

DataSet::DataSet(DataStore& ds, uint8_t level, const std::string& container, const std::string& name) 
: m_impl(std::make_unique<DataSet::Impl>(&ds, level, container, name)) {}

DataSet::DataSet(const DataSet& other)
: m_impl(std::make_unique<DataSet::Impl>(*other.m_impl)) {}

DataSet::DataSet(DataSet&&) = default;

DataSet& DataSet::operator=(const DataSet& other) {
    if(this == &other) return *this;
    m_impl = std::make_unique<DataSet::Impl>(*other.m_impl);
    return *this;
}

DataSet& DataSet::operator=(DataSet&&) = default;

DataSet::~DataSet() {}

DataSet DataSet::next() const {
    if(!valid()) return DataSet();
   
    std::vector<std::string> keys; 
    size_t s = m_impl->m_datastore->nextKeys(
            m_impl->m_level, m_impl->m_container, m_impl->m_name, keys, 1);
    if(s == 0) return DataSet();
    return DataSet(*(m_impl->m_datastore), m_impl->m_level, m_impl->m_container, keys[0]);
}

bool DataSet::valid() const {
    return m_impl && m_impl->m_datastore; 

}

bool DataSet::storeBuffer(const std::string& key, const std::vector<char>& buffer) {
    if(!valid()) {
        throw Exception("Calling store() on invalid DataSet");
    }
    // forward the call to the datastore's store function
    return m_impl->m_datastore->store(0, fullname(), key, buffer);
}

bool DataSet::loadBuffer(const std::string& key, std::vector<char>& buffer) const {
    if(!valid()) {
        throw Exception("Calling load() on invalid DataSet");
    }
    // forward the call to the datastore's load function
    return m_impl->m_datastore->load(0, fullname(), key, buffer);
}

bool DataSet::operator==(const DataSet& other) const {
    return m_impl->m_datastore == other.m_impl->m_datastore
        && m_impl->m_level     == other.m_impl->m_level
        && m_impl->m_container == other.m_impl->m_container
        && m_impl->m_name      == other.m_impl->m_name;
}

bool DataSet::operator!=(const DataSet& other) const {
    return !(*this == other);
}

const std::string& DataSet::name() const {
    return m_impl->m_name;
}

const std::string& DataSet::container() const {
    return m_impl->m_container;
}

std::string DataSet::fullname() const {
    std::stringstream ss;
    if(container().size() != 0)
        ss << container() << "/";
    ss << name();
    return ss.str();
}

DataSet DataSet::createDataSet(const std::string& name) {
    if(name.find('/') != std::string::npos) {
        throw Exception("Invalid character '/' in dataset name");
    }
    m_impl->m_datastore->store(m_impl->m_level+1, fullname(), name, std::vector<char>());
    return DataSet(*(m_impl->m_datastore), 1, fullname(), name);
}

}
