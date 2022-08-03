/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "hepnos/Event.hpp"
#include "hepnos/AsyncEngine.hpp"
#include "hepnos/Prefetcher.hpp"
#include "ItemImpl.hpp"
#include "DataStoreImpl.hpp"
#include "WriteBatchImpl.hpp"
#include "AsyncEngineImpl.hpp"
#include "PrefetcherImpl.hpp"
#include "ProductCacheImpl.hpp"

namespace hepnos {

Event::Event()
: m_impl(std::make_shared<ItemImpl>(nullptr, UUID(), InvalidRunNumber)) {}

Event::Event(std::shared_ptr<ItemImpl>&& impl)
: m_impl(std::move(impl)) { }

Event::Event(const std::shared_ptr<ItemImpl>& impl)
: m_impl(impl) { }

DataStore Event::datastore() const {
    if(!valid()) {
        throw Exception("Calling Event member function on an invalid Event object");
    }
    return DataStore(m_impl->m_datastore);
}

SubRun Event::subrun() const {
    if(!valid()) {
        throw Exception("Calling Event member function on invalid Event object");
    }
    ItemDescriptor subrun_descriptor(
            m_impl->m_descriptor.dataset,
            m_impl->m_descriptor.run,
            m_impl->m_descriptor.subrun);
    return SubRun(std::make_shared<ItemImpl>(m_impl->m_datastore, subrun_descriptor));
}

Event Event::next() const {
    if(!valid()) return Event();

    std::vector<std::shared_ptr<ItemImpl>> next_events;
    size_t s = m_impl->m_datastore->nextItems(ItemType::EVENT, ItemType::SUBRUN, m_impl, next_events, 1);
    if(s == 0) return Event();
    return Event(std::move(next_events[0]));
}

bool Event::valid() const {
    return m_impl && m_impl->m_datastore;

}

ProductID Event::makeProductID(const char* label, size_t label_size,
                               const char* type, size_t type_size) const {
    auto& id = m_impl->m_descriptor;
    return DataStoreImpl::makeProductID(id, label, label_size, type, type_size);
}

ProductID Event::storeRawData(const ProductID& key, const char* value, size_t vsize) {
    if(!valid()) {
        throw Exception("Calling Event member function on an invalid Event object");
    }
    // forward the call to the datastore's store function
    return m_impl->m_datastore->storeRawProduct(key, value, vsize);
}

ProductID Event::storeRawData(WriteBatch& batch, const ProductID& key, const char* value, size_t vsize) {
    if(!valid()) {
        throw Exception("Calling Event member function on an invalid Event object");
    }
    // forward the call to the batch's store function
    if(batch.m_impl)
        return batch.m_impl->storeRawProduct(key, value, vsize);
    else
        return m_impl->m_datastore->storeRawProduct(key, value, vsize);
}

ProductID Event::storeRawData(AsyncEngine& async, const ProductID& key, const char* value, size_t vsize) {
    if(!valid()) {
        throw Exception("Calling Event member function on an invalid Event object");
    }
    // forward the call to the async engine's store function
    if(async.m_impl)
        return async.m_impl->storeRawProduct(key, value, vsize);
    else
        return m_impl->m_datastore->storeRawProduct(key, value, vsize);
}

bool Event::loadRawData(const ProductID& key, std::string& buffer) const {
    if(!valid()) {
        throw Exception("Calling Event member function on an invalid Event object");
    }
    // forward the call to the datastore's load function
    return m_impl->m_datastore->loadRawProduct(key, buffer);
}

bool Event::loadRawData(const ProductID& key, char* value, size_t* vsize) const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    // forward the call to the datastore's load function
    return m_impl->m_datastore->loadRawProduct(key, value, vsize);
}

bool Event::loadRawData(const Prefetcher& prefetcher, const ProductID& key, std::string& buffer) const {
    if(!valid()) {
        throw Exception("Calling Event member function on an invalid Event object");
    }
    return prefetcher.m_impl->loadRawProduct(key, buffer);
}

bool Event::loadRawData(const Prefetcher& prefetcher, const ProductID& key, char* value, size_t* vsize) const {
    if(!valid()) {
        throw Exception("Calling Event member function on an invalid Event");
    }
    return prefetcher.m_impl->loadRawProduct(key, value, vsize);
}

bool Event::loadRawData(const ProductCache& cache, const ProductID& key, std::string& buffer) const {
    if(!valid()) {
        throw Exception("Calling Event member function on an invalid Event object");
    }
    return cache.m_impl->loadRawProduct(key, buffer);
}

bool Event::loadRawData(const ProductCache& cache, const ProductID& key, char* value, size_t* vsize) const {
    if(!valid()) {
        throw Exception("Calling Event member function on an invalid Event");
    }
    return cache.m_impl->loadRawProduct(key, value, vsize);
}

std::vector<ProductID> Event::listProducts(const std::string& label) const {
    if(!valid()) {
        throw Exception("Calling Event member function on an invalid Event object");
    }
    // forward the call to the datastore's listProducts function
    auto& id = m_impl->m_descriptor;
    return m_impl->m_datastore->listProducts(id, label);
}

bool Event::operator==(const Event& other) const {
    bool v1 = valid();
    bool v2 = other.valid();
    if(!v1 && !v2) return true;
    if(!v1 &&  v2) return false;
    if(v1  && !v2) return false;
    return (m_impl == other.m_impl) || (*m_impl == *other.m_impl);
}

bool Event::operator!=(const Event& other) const {
    return !(*this == other);
}

EventNumber Event::number() const {
    if(!valid()) {
        throw Exception("Calling Event member function on an invalid Event object");
    }
    return m_impl->m_descriptor.event;
}

void Event::toDescriptor(EventDescriptor& descriptor) {
    std::memset(descriptor.data, 0, sizeof(descriptor.data));
    if(!valid()) return;
    std::memcpy(descriptor.data, &(m_impl->m_descriptor), sizeof(descriptor.data));
}

Event Event::fromDescriptor(const DataStore& datastore, const EventDescriptor& descriptor, bool validate) {
    auto itemImpl = std::make_shared<ItemImpl>(datastore.m_impl, UUID(), InvalidRunNumber);
    auto& itemDescriptor = itemImpl->m_descriptor;
    std::memcpy(&itemDescriptor, descriptor.data, sizeof(descriptor.data));
    if((!validate) || datastore.m_impl->itemExists(itemDescriptor))
        return Event(std::move(itemImpl));
    else return Event();
}

}
