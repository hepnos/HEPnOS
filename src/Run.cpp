#include "hepnos/Run.hpp"
#include "private/RunImpl.hpp"
#include "private/DataStoreImpl.hpp"

namespace hepnos {

Run::Run()
: m_impl(std::make_unique<Run::Impl>(nullptr, 0, "", 0)) {} 

Run::Run(DataStore* ds, uint8_t level, const std::string& container, const RunNumber& rn)
: m_impl(std::make_unique<Run::Impl>(ds, level, container, rn)) { }

Run::Run(const Run& other)
: m_impl(std::make_unique<Run::Impl>(*other.m_impl)) {}

Run::Run(Run&&) = default;

Run& Run::operator=(const Run& other) {
    if(this == &other) return *this;
    m_impl = std::make_unique<Run::Impl>(*other.m_impl);
    return *this;
}

Run& Run::operator=(Run&&) = default;

Run::~Run() = default; 

Run Run::next() const {
    if(!valid()) return Run();
   
    std::vector<std::string> keys;
    size_t s = m_impl->m_datastore->m_impl->nextKeys(
            m_impl->m_level, m_impl->m_container, 
            m_impl->makeKeyStringFromRunNumber(), keys, 1);
    if(s == 0) return Run();
    size_t i = m_impl->m_container.size()+1;
    if(keys[0].size() <= i) return Run();
    if(keys[0][i] != '%') return Run();
    std::stringstream strRunNumber;
    strRunNumber << std::hex << keys[0].substr(i+1);
    RunNumber rn;
    strRunNumber >> rn;

    return Run(m_impl->m_datastore, m_impl->m_level, m_impl->m_container, rn);
}

bool Run::valid() const {
    return m_impl && m_impl->m_datastore; 

}

bool Run::storeRawData(const std::string& key, const std::vector<char>& buffer) {
    if(!valid()) {
        throw Exception("Calling store() on invalid Run");
    }
    // forward the call to the datastore's store function
    return m_impl->m_datastore->m_impl->store(0, m_impl->fullpath(), key, buffer);
}

bool Run::loadRawData(const std::string& key, std::vector<char>& buffer) const {
    if(!valid()) {
        throw Exception("Calling load() on invalid Run");
    }
    // forward the call to the datastore's load function
    return m_impl->m_datastore->m_impl->load(0, m_impl->fullpath(), key, buffer);
}

bool Run::operator==(const Run& other) const {
    return m_impl->m_datastore == other.m_impl->m_datastore
        && m_impl->m_level     == other.m_impl->m_level
        && m_impl->m_container == other.m_impl->m_container
        && m_impl->m_run_nr    == other.m_impl->m_run_nr;
}

bool Run::operator!=(const Run& other) const {
    return !(*this == other);
}

const RunNumber& Run::number() const {
    return m_impl->m_run_nr;
}

const std::string& Run::container() const {
    return m_impl->m_container;
}

/*
DataSet DataSet::createDataSet(const std::string& name) {
    if(name.find('/') != std::string::npos) {
        throw Exception("Invalid character '/' in dataset name");
    }
    std::string parent = fullname();
    m_impl->m_datastore->m_impl->store(m_impl->m_level+1, parent, name, std::vector<char>());
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
    bool b = m_impl->m_datastore->m_impl->load(m_impl->m_level+1, parent, datasetName, data);
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
*/
}
