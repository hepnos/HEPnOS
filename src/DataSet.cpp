#include "hepnos/DataSet.hpp"

namespace hepnos {

DataSet::DataSet()
: m_datastore(nullptr)
, m_level(0)
, m_container("")
, m_name("") {}

DataSet::DataSet(DataStore& ds, uint8_t level, const std::string& fullname)
: m_datastore(&ds)
, m_level(level) {
    size_t p = fullname.find_last_of('/');
    if(p == std::string::npos) {
        m_name = fullname;
    } else {
        m_name = fullname.substr(p+1);
        m_container = fullname.substr(0, p);
    }
}

DataSet::DataSet(DataStore& ds, uint8_t level, const std::string& container, const std::string& name) 
: m_datastore(&ds)
, m_level(level)
, m_container(container)
, m_name(name) {}

DataSet DataSet::next() const {
    if(!valid()) return DataSet();
   
    std::vector<std::string> keys; 
    size_t s = m_datastore->nextKeys(m_level, m_container, m_name, keys, 1);
    if(s == 0) return DataSet();
    return DataSet(*m_datastore, m_level, m_container, keys[0]);
}

bool DataSet::valid() const {
    return m_datastore != nullptr;

}

bool DataSet::storeBuffer(const std::string& key, const std::vector<char>& buffer) {
    if(!valid()) {
        throw Exception("Calling store() on invalid DataSet");
    }
    // forward the call to the datastore's store function
    return m_datastore->store(0, fullname(), key, buffer);
}

bool DataSet::loadBuffer(const std::string& key, std::vector<char>& buffer) const {
    if(!valid()) {
        throw Exception("Calling load() on invalid DataSet");
    }
    // forward the call to the datastore's load function
    std::stringstream ss;
    if(m_container.size() != 0)
        ss << m_container << "/";
    ss << m_name;
    return m_datastore->load(0, fullname(), key, buffer);
}

bool DataSet::operator==(const DataSet& other) const {
    return m_datastore == other.m_datastore
        && m_level     == other.m_level
        && m_container == other.m_container
        && m_name      == other.m_name;
}

}
