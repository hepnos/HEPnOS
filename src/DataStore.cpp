/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include <vector>
#include <functional>
#include <iostream>
#include "hepnos/Exception.hpp"
#include "hepnos/DataStore.hpp"
#include "hepnos/DataSet.hpp"
#include "hepnos/WriteBatch.hpp"
#include "DataSetImpl.hpp"
#include "DataStoreImpl.hpp"
#include "WriteBatchImpl.hpp"

namespace hepnos {

////////////////////////////////////////////////////////////////////////////////////////////
// DataStore implementation
////////////////////////////////////////////////////////////////////////////////////////////

DataStore::DataStore(std::shared_ptr<DataStoreImpl>&& impl)
: m_impl(std::move(impl)) {}

DataStore::DataStore(const std::shared_ptr<DataStoreImpl>& impl)
: m_impl(impl) {}

bool DataStore::valid() const {
    return m_impl != nullptr;
}

DataStore DataStore::connect() { 
    char* file = getenv("HEPNOS_CONFIG_FILE");
    if(file == nullptr) 
        throw Exception("HEPNOS_CONFIG_FILE environment variable not set");
    std::string configFile(file);
    auto impl = std::make_shared<DataStoreImpl>();
    impl->init(configFile);
    return DataStore(std::move(impl));
}

DataStore DataStore::connect(const std::string& configFile) {
    auto impl = std::make_shared<DataStoreImpl>();
    impl->init(configFile);
    return DataStore(std::move(impl));
}

DataStore::iterator DataStore::find(const std::string& datasetPath) {
    if(!m_impl) {
        throw Exception("Calling DataStore member function on an invalid DataStore object");
    }

    int ret;

    if(datasetPath.find('%') != std::string::npos) {
        throw Exception("Invalid character ('%') in dataset name");
    }

    size_t slash_count = std::count(datasetPath.begin(), datasetPath.end(), '/');
    size_t level = 1 + slash_count;
    std::string containerName;
    std::string datasetName;

    if(slash_count == 0) {
        datasetName = datasetPath;
        containerName = "";
    } else {
        size_t c = datasetPath.find_last_of('/');
        containerName = datasetPath.substr(0,c);
        datasetName   = datasetPath.substr(c+1);
    }

    std::string data;
    bool b = m_impl->load(level, containerName, datasetName, data);
    if(!b) {
        return m_impl->m_end;
    }
    return iterator(
            DataSet(
                std::make_shared<DataSetImpl>(
                    m_impl, level, std::make_shared<std::string>(containerName), datasetName)));
}

DataSet DataStore::operator[](const std::string& datasetName) const {
    auto it = find(datasetName);
    if(!it->valid())
        throw Exception("Requested DataSet does not exist");
    return std::move(*it);
}

DataStore::const_iterator DataStore::find(const std::string& datasetName) const {
    DataStore::iterator it = const_cast<DataStore*>(this)->find(datasetName);
    return it;
}

DataStore::iterator DataStore::begin() {
    if(!m_impl) {
        throw Exception("Calling DataStore member function on an invalid DataStore object");
    }
    DataSet ds(
            std::make_shared<DataSetImpl>(
                m_impl, 1, std::make_shared<std::string>(""), ""));
    ds = ds.next();
    if(ds.valid()) return iterator(std::move(ds));
    else return end();
}

DataStore::const_iterator DataStore::begin() const {
    iterator it = const_cast<DataStore*>(this)->begin();
    return const_iterator(std::move(it));
}

DataStore::iterator DataStore::end() {
    if(!m_impl) {
        throw Exception("Calling DataStore member function on an invalid DataStore object");
    }
    return m_impl->m_end;
}

DataStore::const_iterator DataStore::end() const {
    if(!m_impl) {
        throw Exception("Calling DataStore member function on an invalid DataStore object");
    }
    return m_impl->m_end;
}

DataStore::const_iterator DataStore::cbegin() const {
    return const_cast<DataStore*>(this)->begin();
}

DataStore::const_iterator DataStore::cend() const {
    return const_cast<DataStore*>(this)->end();
}

DataStore::iterator DataStore::lower_bound(const std::string& lb) {
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
                m_impl, 1, std::make_shared<std::string>(""), lb2));
    ds = ds.next();
    if(!ds.valid()) return end();
    else return iterator(std::move(ds));
}

DataStore::const_iterator DataStore::lower_bound(const std::string& lb) const {
    iterator it = const_cast<DataStore*>(this)->lower_bound(lb);
    return it;
}

DataStore::iterator DataStore::upper_bound(const std::string& ub) {
    if(!m_impl) {
        throw Exception("Calling DataStore member function on an invalid DataStore object");
    }
    DataSet ds(
            std::make_shared<DataSetImpl>(
                m_impl, 1, std::make_shared<std::string>(""), ub));
    ds = ds.next();
    if(!ds.valid()) return end();
    else return iterator(std::move(ds));
}

DataStore::const_iterator DataStore::upper_bound(const std::string& ub) const {
    iterator it = const_cast<DataStore*>(this)->upper_bound(ub);
    return it;
}

DataSet DataStore::createDataSet(const std::string& name) {
    if(!m_impl) {
        throw Exception("Calling DataStore member function on an invalid DataStore object");
    }
    if(name.find('/') != std::string::npos
    || name.find('%') != std::string::npos) {
        throw Exception("Invalid character ('/' or '%') in dataset name");
    }
    m_impl->store(1, "", name);
    return DataSet(
            std::make_shared<DataSetImpl>(
                m_impl, 1, std::make_shared<std::string>(""), name));
}

DataSet DataStore::createDataSet(WriteBatch& batch, const std::string& name) {
    if(!m_impl) {
        throw Exception("Calling DataStore member function on an invalid DataStore object");
    }
    if(name.find('/') != std::string::npos
    || name.find('%') != std::string::npos) {
        throw Exception("Invalid character ('/' or '%') in dataset name");
    }
    batch.m_impl->store(1, "", name);
    return DataSet(
            std::make_shared<DataSetImpl>(
                m_impl, 1, std::make_shared<std::string>(""), name));
}

void DataStore::shutdown() {
    if(!m_impl) {
        throw Exception("Calling DataStore member function on an invalid DataStore object");
    }
    for(auto addr : m_impl->m_addrs) {
        margo_shutdown_remote_instance(m_impl->m_mid, addr.second);
    }
}

bool DataStore::loadRawProduct(const ProductID& productID, std::string& buffer) {
    if(!m_impl) {
        throw Exception("Calling DataStore member function on an invalid DataStore object");
    }
    return m_impl->load(productID.m_level, productID.m_containerName, productID.m_objectName, buffer);
}

bool DataStore::loadRawProduct(const ProductID& productID, char* data, size_t* size) {
    if(!m_impl) {
        throw Exception("Calling DataStore member function on an invalid DataStore object");
    }
    return m_impl->load(productID.m_level, productID.m_containerName, productID.m_objectName, data, size);
}

////////////////////////////////////////////////////////////////////////////////////////////
// DataStore::const_iterator::Impl implementation
////////////////////////////////////////////////////////////////////////////////////////////

class DataStore::const_iterator::Impl {
    public:
        DataSet    m_current_dataset;

        Impl()
        : m_current_dataset()
        {}

        Impl(const DataSet& dataset)
        : m_current_dataset(dataset)
        {}

        Impl(DataSet&& dataset)
        : m_current_dataset(std::move(dataset))
        {}

        Impl(const Impl& other)
        : m_current_dataset(other.m_current_dataset) 
        {}

        bool operator==(const Impl& other) const {
            return m_current_dataset == other.m_current_dataset;
        }
};

////////////////////////////////////////////////////////////////////////////////////////////
// DataStore::const_iterator::Impl implementation
////////////////////////////////////////////////////////////////////////////////////////////

DataStore::const_iterator::const_iterator()
: m_impl(std::make_unique<Impl>()) {}

DataStore::const_iterator::const_iterator(const DataSet& dataset)
: m_impl(std::make_unique<Impl>(dataset)) {}

DataStore::const_iterator::const_iterator(DataSet&& dataset)
: m_impl(std::make_unique<Impl>(std::move(dataset))) {}

DataStore::const_iterator::~const_iterator() {}

DataStore::const_iterator::const_iterator(const DataStore::const_iterator& other) {
    if(other.m_impl) {
        m_impl = std::make_unique<Impl>(*other.m_impl);
    }
}

DataStore::const_iterator::const_iterator(DataStore::const_iterator&& other) 
: m_impl(std::move(other.m_impl)) {}

DataStore::const_iterator& DataStore::const_iterator::operator=(const DataStore::const_iterator& other) {
    if(&other == this) return *this;
    if(other.m_impl)
        m_impl = std::make_unique<Impl>(*other.m_impl);
    else
        m_impl.reset();
    return *this;
}

DataStore::const_iterator& DataStore::const_iterator::operator=(DataStore::const_iterator&& other) {
    if(&other == this) return *this;
    m_impl = std::move(other.m_impl);
    return *this;
}

DataStore::const_iterator::self_type DataStore::const_iterator::operator++() {
    if(!m_impl) {
        throw Exception("Trying to increment an invalid iterator");
    }
    m_impl->m_current_dataset = m_impl->m_current_dataset.next();
    return *this;
}

DataStore::const_iterator::self_type DataStore::const_iterator::operator++(int) {
    const_iterator copy = *this;
    ++(*this);
    return copy;
}

const DataStore::const_iterator::reference DataStore::const_iterator::operator*() {
    if(!m_impl) {
        throw Exception("Trying to dereference an invalid iterator");
    }
    return m_impl->m_current_dataset;
}

const DataStore::const_iterator::pointer DataStore::const_iterator::operator->() {
    if(!m_impl) {
        throw Exception("Trying to dereference an invalid iterator");
    }
    return &(m_impl->m_current_dataset);
}

bool DataStore::const_iterator::operator==(const self_type& rhs) const {
    if(!m_impl && !rhs.m_impl)  return true;
    if(m_impl  && !rhs.m_impl)  return false;
    if(!m_impl && rhs.m_impl)   return false;
    return *m_impl == *(rhs.m_impl);
}

bool DataStore::const_iterator::operator!=(const self_type& rhs) const {
    return !(*this == rhs);
}

////////////////////////////////////////////////////////////////////////////////////////////
// DataStore::iterator implementation
////////////////////////////////////////////////////////////////////////////////////////////

DataStore::iterator::iterator(const DataSet& current)
: const_iterator(current) {}

DataStore::iterator::iterator(DataSet&& current)
: const_iterator(std::move(current)) {}

DataStore::iterator::iterator()
: const_iterator() {}

DataStore::iterator::~iterator() {}

DataStore::iterator::iterator(const DataStore::iterator& other)
: const_iterator(other) {}

DataStore::iterator::iterator(DataStore::iterator&& other) 
: const_iterator(std::move(other)) {}

DataStore::iterator& DataStore::iterator::operator=(const DataStore::iterator& other) {
    if(this == &other) return *this;
    if(other.m_impl)
        m_impl = std::make_unique<Impl>(*other.m_impl);
    else
        m_impl.reset();
    return *this;
}

DataStore::iterator& DataStore::iterator::operator=(DataStore::iterator&& other) {
    if(this == &other) return *this;
    m_impl = std::move(other.m_impl);
    return *this;
}

DataStore::iterator::reference DataStore::iterator::operator*() {
    return const_cast<reference>(const_iterator::operator*());
}

DataStore::iterator::pointer DataStore::iterator::operator->() {
    return const_cast<pointer>(const_iterator::operator->());
}

}

