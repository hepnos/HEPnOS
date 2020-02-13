/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "hepnos/DataSet.hpp"
#include "hepnos/Run.hpp"
#include "hepnos/RunSet.hpp"
#include "RunImpl.hpp"
#include "DataSetImpl.hpp"
#include "DataStoreImpl.hpp"
#include "WriteBatchImpl.hpp"

namespace hepnos {

DataSet::DataSet()
: m_impl(std::make_shared<DataSetImpl>(nullptr, 0, std::make_shared<std::string>(""), "")) {}

DataSet::DataSet(const std::shared_ptr<DataSetImpl>& impl)
: m_impl(impl) {}

DataSet::DataSet(std::shared_ptr<DataSetImpl>&& impl)
: m_impl(std::move(impl)) {}

DataStore DataSet::datastore() const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    return DataStore(m_impl->m_datastore);
}

DataSet DataSet::next() const {
    if(!valid()) return DataSet();
   
    std::vector<std::string> keys; 
    size_t s = m_impl->m_datastore->nextKeys(
            m_impl->m_level, *m_impl->m_container, m_impl->m_name, keys, 1);
    if(s == 0) return DataSet();
    return DataSet(std::make_shared<DataSetImpl>(m_impl->m_datastore, m_impl->m_level, keys[0]));
}

bool DataSet::valid() const {
    return m_impl && m_impl->m_datastore; 
}

ProductID DataSet::storeRawData(const std::string& key, const char* value, size_t vsize) {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    // forward the call to the datastore's store function
    return m_impl->m_datastore->store(0, fullname(), key, value, vsize);
}

ProductID DataSet::storeRawData(WriteBatch& batch, const std::string& key, const char* value, size_t vsize) {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    // forward the call to the datastore's store function
    return batch.m_impl->store(0, fullname(), key, value, vsize);
}

bool DataSet::loadRawData(const std::string& key, std::string& buffer) const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    // forward the call to the datastore's load function
    return m_impl->m_datastore->load(0, fullname(), key, buffer);
}

bool DataSet::loadRawData(const std::string& key, char* value, size_t* vsize) const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    // forward the call to the datastore's load function
    return m_impl->m_datastore->load(0, fullname(), key, value, vsize);
}

bool DataSet::operator==(const DataSet& other) const {
    bool v1 = valid();
    bool v2 = other.valid();
    if(!v1 && !v2) return true;
    if(v1  && !v2) return false;
    if(!v2 &&  v2) return false;
    return m_impl->m_datastore  == other.m_impl->m_datastore
        && m_impl->m_level      == other.m_impl->m_level
        && *m_impl->m_container == *other.m_impl->m_container
        && m_impl->m_name       == other.m_impl->m_name;
}

bool DataSet::operator!=(const DataSet& other) const {
    return !(*this == other);
}

const std::string& DataSet::name() const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    return m_impl->m_name;
}

const std::string& DataSet::container() const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    return *m_impl->m_container;
}

std::string DataSet::fullname() const {
    std::stringstream ss;
    if(container().size() != 0)
        ss << container() << "/";
    ss << name();
    return ss.str();
}

DataSet DataSet::createDataSet(const std::string& name) {
    if(name.find('/') != std::string::npos
    || name.find('%') != std::string::npos) {
        throw Exception("Invalid character '/' or '%' in dataset name");
    }
    std::string parent = fullname();
    m_impl->m_datastore->store(m_impl->m_level+1, parent, name);
    return DataSet(std::make_shared<DataSetImpl>(
                m_impl->m_datastore, m_impl->m_level+1,
                std::make_shared<std::string>(parent), name));
}

Run DataSet::createRun(const RunNumber& runNumber) {
    if(InvalidRunNumber == runNumber) {
        throw Exception("Trying to create a Run with InvalidRunNumber");
    }
    std::string parent = fullname();
    std::string runStr = makeKeyStringFromNumber(runNumber);
    m_impl->m_datastore->store(m_impl->m_level+1, parent, runStr);
    return Run(
            std::make_shared<RunImpl>(
                m_impl->m_datastore, m_impl->m_level+1,
                std::make_shared<std::string>(parent), runNumber));
}

Run DataSet::createRun(WriteBatch& batch, const RunNumber& runNumber) {
    if(InvalidRunNumber == runNumber) {
        throw Exception("Trying to create a Run with InvalidRunNumber");
    }
    std::string parent = fullname();
    std::string runStr = makeKeyStringFromNumber(runNumber);
    batch.m_impl->store(m_impl->m_level+1, parent, runStr);
    return Run(
            std::make_shared<RunImpl>(
                m_impl->m_datastore, m_impl->m_level+1,
                std::make_shared<std::string>(parent), runNumber));
}

DataSet DataSet::operator[](const std::string& datasetName) const {
    auto it = find(datasetName);
    if(!it->valid())
        throw Exception("Requested DataSet does not exist");
    return std::move(*it);
}

Run DataSet::operator[](const RunNumber& runNumber) const {
    auto it = runs().find(runNumber);
    if(!it->valid())
        throw Exception("Requested Run does not exist");
    return std::move(*it);
}

DataSet::iterator DataSet::find(const std::string& datasetPath) {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    int ret;
    if(datasetPath.find('%') != std::string::npos) {
        throw Exception("Invalid character '%' in dataset name");
    }

    size_t slash_count = std::count(datasetPath.begin(), datasetPath.end(), '/');
    size_t level = m_impl->m_level + 1 + slash_count;
    std::string containerName;
    std::string datasetName;

    std::string parent = fullname();

    if(slash_count == 0) {
        datasetName = datasetPath;
        containerName = parent;
    } else {
        size_t c = datasetPath.find_last_of('/');
        containerName = parent + "/" + datasetPath.substr(0,c);
        datasetName   = datasetPath.substr(c+1);
    }

    bool b = m_impl->m_datastore->exists(level, containerName, datasetName);
    if(!b) {
        return m_impl->m_datastore->m_end;
    }
    return iterator(
                DataSet(
                    std::make_shared<DataSetImpl>(
                        m_impl->m_datastore,
                        level,
                        std::make_shared<std::string>(containerName),
                        datasetName)));
}

DataSet::const_iterator DataSet::find(const std::string& datasetName) const {
    iterator it = const_cast<DataSet*>(this)->find(datasetName);
    return it;
}

DataSet::iterator DataSet::begin() {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    // we use the prefix "&" because we need something that comes after "%"
    // (which represents runs) and is not going to be in a dataset name
    DataSet ds(
            std::make_shared<DataSetImpl>(
                m_impl->m_datastore, 
                m_impl->m_level+1,
                std::make_shared<std::string>(fullname()),
                "&"));

    ds = ds.next();
    if(ds.valid()) return iterator(ds);
    else return end();
}

DataSet::iterator DataSet::end() {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    return m_impl->m_datastore->m_end;
}

DataSet::const_iterator DataSet::begin() const {
    return const_iterator(const_cast<DataSet*>(this)->begin());
}

DataSet::const_iterator DataSet::end() const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    return m_impl->m_datastore->m_end;
}

DataSet::const_iterator DataSet::cbegin() const {
    return const_iterator(const_cast<DataSet*>(this)->begin());
}

DataSet::const_iterator DataSet::cend() const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    return m_impl->m_datastore->m_end;
}

DataSet::iterator DataSet::lower_bound(const std::string& lb) {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    std::string lb2 = lb;
    size_t s = lb2.size();
    lb2[s-1] -= 1; // sdskv_list_keys's start_key is exclusive
    iterator it = find(lb2);
    if(it != end()) {
        // we found something before the specified lower bound
        ++it;
        return it;
    }
    DataSet ds(
            std::make_shared<DataSetImpl>(
                m_impl->m_datastore, 
                m_impl->m_level+1,
                std::make_shared<std::string>(fullname()),
                lb2));

    ds = ds.next();
    if(!ds.valid()) return end();
    else return iterator(ds);
}

DataSet::const_iterator DataSet::lower_bound(const std::string& lb) const {
    iterator it = const_cast<DataSet*>(this)->lower_bound(lb);
    return it;
}

DataSet::iterator DataSet::upper_bound(const std::string& ub) {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    DataSet ds(
            std::make_shared<DataSetImpl>(
                m_impl->m_datastore,
                m_impl->m_level+1,
                std::make_shared<std::string>(fullname()), ub));

    ds = ds.next();
    if(!ds.valid()) return end();
    else return iterator(ds);
}

DataSet::const_iterator DataSet::upper_bound(const std::string& ub) const {
    iterator it = const_cast<DataSet*>(this)->upper_bound(ub);
    return it;
}

RunSet DataSet::runs() const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    return RunSet(m_impl);
}

}
