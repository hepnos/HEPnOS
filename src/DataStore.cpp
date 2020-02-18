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

DataSet DataStore::root() const {
    return DataSet(std::make_shared<DataSetImpl>(m_impl, 0, std::make_shared<std::string>(""), ""));
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

}

