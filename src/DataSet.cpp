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

DataSet::DataSet(DataStore* ds, uint8_t level, const std::string& fullname)
: m_impl(std::make_unique<DataSet::Impl>(ds, level, "", "")) {
    size_t p = fullname.find_last_of('/');
    if(p == std::string::npos) {
        m_impl->m_name = fullname;
    } else {
        m_impl->m_name = fullname.substr(p+1);
        m_impl->m_container = fullname.substr(0, p);
    }
}

DataSet::DataSet(DataStore* ds, uint8_t level, const std::string& container, const std::string& name) 
: m_impl(std::make_unique<DataSet::Impl>(ds, level, container, name)) {}

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
    return DataSet(m_impl->m_datastore, m_impl->m_level, m_impl->m_container, keys[0]);
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
    std::string parent = fullname();
    m_impl->m_datastore->store(m_impl->m_level+1, parent, name, std::vector<char>());
    return DataSet(m_impl->m_datastore, m_impl->m_level+1, parent, name);
}

DataSet DataSet::operator[](const std::string& datasetName) const {
    auto it = find(datasetName);
    return std::move(*it);
}

DataSet::iterator DataSet::find(const std::string& datasetName) {
    int ret;
    if(datasetName.find('/') != std::string::npos) {
        throw Exception("Invalid character '/' in dataset name");
    }
    std::vector<char> data;
    std::string parent = fullname();
    bool b = m_impl->m_datastore->load(m_impl->m_level+1, parent, datasetName, data);
    if(!b) {
        return m_impl->m_datastore->end();
    }
    return iterator(DataSet(m_impl->m_datastore, m_impl->m_level+1, parent, datasetName));
}

DataSet::const_iterator DataSet::find(const std::string& datasetName) const {
    iterator it = const_cast<DataSet*>(this)->find(datasetName);
    return it;
}

DataSet::iterator DataSet::begin() {
    DataSet ds(m_impl->m_datastore, m_impl->m_level+1, fullname(),"");
    ds = ds.next();
    if(ds.valid()) return iterator(ds);
    else return end();
}

DataSet::iterator DataSet::end() {
    return m_impl->m_datastore->end();
}

DataSet::const_iterator DataSet::cbegin() const {
    return const_iterator(const_cast<DataSet*>(this)->begin());
}

DataSet::const_iterator DataSet::cend() const {
    return m_impl->m_datastore->cend();
}

DataSet::iterator DataSet::lower_bound(const std::string& lb) {
    std::string lb2 = lb;
    size_t s = lb2.size();
    lb2[s-1] -= 1; // sdskv_list_keys's start_key is exclusive
    iterator it = find(lb2);
    if(it != end()) {
        // we found something before the specified lower bound
        ++it;
        return it;
    }
    DataSet ds(m_impl->m_datastore, m_impl->m_level+1, fullname(), lb2);
    ds = ds.next();
    if(!ds.valid()) return end();
    else return iterator(ds);
}

DataSet::const_iterator DataSet::lower_bound(const std::string& lb) const {
    iterator it = const_cast<DataSet*>(this)->lower_bound(lb);
    return it;
}

DataSet::iterator DataSet::upper_bound(const std::string& ub) {
    DataSet ds(m_impl->m_datastore, m_impl->m_level+1, fullname(), ub);
    ds = ds.next();
    if(!ds.valid()) return end();
    else return iterator(ds);
}

DataSet::const_iterator DataSet::upper_bound(const std::string& ub) const {
    iterator it = const_cast<DataSet*>(this)->upper_bound(ub);
    return it;
}

}
