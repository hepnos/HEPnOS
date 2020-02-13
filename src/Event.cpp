/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "hepnos/Event.hpp"
#include "EventImpl.hpp"
#include "DataStoreImpl.hpp"
#include "WriteBatchImpl.hpp"

namespace hepnos {

Event::Event()
: m_impl(std::make_shared<EventImpl>(0, nullptr, InvalidEventNumber)) {} 

Event::Event(std::shared_ptr<EventImpl>&& impl)
: m_impl(std::move(impl)) { }

Event::Event(const std::shared_ptr<EventImpl>& impl)
: m_impl(impl) { }

DataStore Event::datastore() const {
    if(!valid()) {
        throw Exception("Calling Event member function on an invalid Event object");
    }
    return DataStore(m_impl->m_datastore);
}

Event Event::next() const {
    if(!valid()) return Event();
   
    std::vector<std::string> keys;
    auto container = m_impl->container();
    size_t s = m_impl->m_datastore->nextKeys(
            m_impl->m_level, container, 
            m_impl->makeKeyStringFromEventNumber(), keys, 1);
    if(s == 0) return Event();
    size_t i = container.size()+1;
    if(keys[0].size() <= i) return Event();
    EventNumber n = parseNumberFromKeyString<EventNumber>(&keys[0][i]);
    if(n == InvalidEventNumber) return Event();
    return Event(std::make_shared<EventImpl>(m_impl->m_level, m_impl->m_subrun, n));
}

bool Event::valid() const {
    return m_impl && m_impl->m_datastore; 

}

ProductID Event::storeRawData(const std::string& key, const char* value, size_t vsize) {
    if(!valid()) {
        throw Exception("Calling Event member function on an invalid Event object");
    }
    // forward the call to the datastore's store function
    return m_impl->m_datastore->store(0, m_impl->fullpath(), key, value, vsize);
}

ProductID Event::storeRawData(WriteBatch& batch, const std::string& key, const char* value, size_t vsize) {
    if(!valid()) {
        throw Exception("Calling Event member function on an invalid Event object");
    }
    // forward the call to the datastore's store function
    return batch.m_impl->store(0, m_impl->fullpath(), key, value, vsize);
}

bool Event::loadRawData(const std::string& key, std::string& buffer) const {
    if(!valid()) {
        throw Exception("Calling Event member function on an invalid Event object");
    }
    // forward the call to the datastore's load function
    return m_impl->m_datastore->load(0, m_impl->fullpath(), key, buffer);
}

bool Event::loadRawData(const std::string& key, char* value, size_t* vsize) const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    // forward the call to the datastore's load function
    return m_impl->m_datastore->load(0, m_impl->fullpath(), key, value, vsize);
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

const EventNumber& Event::number() const {
    if(!valid()) {
        throw Exception("Calling Event member function on an invalid Event object");
    }
    return m_impl->m_event_number;
}

}
