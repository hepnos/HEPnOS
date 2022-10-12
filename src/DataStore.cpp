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

DataStore DataStore::connect(const std::string& protocol,
                             const std::string& hepnosFile,
                             const std::string& margoFile) {
    auto impl = std::make_shared<DataStoreImpl>();
    impl->init(protocol, hepnosFile, margoFile);
    return DataStore(std::move(impl));
}

void DataStore::shutdown() {
    if(!m_impl) {
        throw Exception("Calling DataStore member function on an invalid DataStore object");
    }
    for(auto addr : m_impl->m_addrs) {
        m_impl->m_engine.shutdown_remote_engine(addr.second);
    }
}

ProductID DataStore::storeRawData(const ProductID& productID, const char* value, size_t vsize) {
    if(!m_impl) {
        throw Exception("Calling DataStore member function on an invalid DataStore object");
    }
    return m_impl->storeRawProduct(productID, value, vsize);
}

bool DataStore::loadRawData(const ProductID& productID, std::string& buffer) const {
    if(!m_impl) {
        throw Exception("Calling DataStore member function on an invalid DataStore object");
    }
    return m_impl->loadRawProduct(productID, buffer);
}

bool DataStore::loadRawData(const ProductID& productID, char* data, size_t* size) const {
    if(!m_impl) {
        throw Exception("Calling DataStore member function on an invalid DataStore object");
    }
    return m_impl->loadRawProduct(productID, data, size);
}

size_t DataStore::numTargets(const ItemType& type) const {
    if(!m_impl) {
        throw Exception("Calling DataStore member function on an invalid DataStore object");
    }
    return m_impl->numTargets(type);
}

void DataStore::createQueueImpl(const std::string& name,
                                const std::string& type_name) {
    if(!m_impl) {
        throw Exception("Calling DataStore member function on an invalid DataStore object");
    }
    m_impl->createQueue(name, type_name);
}

Queue DataStore::openQueueImpl(const std::string& name,
                               const std::string& type_name,
                               const std::type_info& type_info,
                               QueueAccessMode mode) {
    if(!m_impl) {
        throw Exception("Calling DataStore member function on an invalid DataStore object");
    }
    return Queue(m_impl->openQueue(name, type_name, type_info, mode));
}

void DataStore::destroyQueueImpl(const std::string& name,
                                 const std::string& type_name) {
    if(!m_impl) {
        throw Exception("Calling DataStore member function on an invalid DataStore object");
    }
    return m_impl->destroyQueue(name, type_name);
}

}

