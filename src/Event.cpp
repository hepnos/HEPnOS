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
: m_impl(std::make_unique<Impl>(nullptr, 0, std::make_shared<std::string>(""), InvalidEventNumber)) {} 

Event::Event(DataStore* ds, uint8_t level, const std::shared_ptr<std::string>& container, const EventNumber& rn)
: m_impl(std::make_unique<Impl>(ds, level, container, rn)) { }

Event::Event(const Event& other) {
    if(other.m_impl)
        m_impl = std::make_unique<Impl>(*other.m_impl);
}

Event::Event(Event&&) = default;

Event& Event::operator=(const Event& other) {
    if(this == &other) return *this;
    if(other.m_impl)
        m_impl = std::make_unique<Impl>(*other.m_impl);
    return *this;
}

Event& Event::operator=(Event&&) = default;

Event::~Event() = default; 

DataStore* Event::getDataStore() const {
    if(!valid()) {
        throw Exception("Calling Event member function on an invalid Event object");
    }
    return m_impl->m_datastore;
}

Event Event::next() const {
    if(!valid()) return Event();
   
    std::vector<std::string> keys;
    size_t s = m_impl->m_datastore->m_impl->nextKeys(
            m_impl->m_level, *m_impl->m_container, 
            m_impl->makeKeyStringFromEventNumber(), keys, 1);
    if(s == 0) return Event();
    size_t i = m_impl->m_container->size()+1;
    if(keys[0].size() <= i) return Event();
    EventNumber n = parseNumberFromKeyString<EventNumber>(&keys[0][i]);
    if(n == InvalidEventNumber) return Event();
    return Event(m_impl->m_datastore, m_impl->m_level, m_impl->m_container, n);
}

bool Event::valid() const {
    return m_impl && m_impl->m_datastore; 

}

ProductID Event::storeRawData(const std::string& key, const char* value, size_t vsize) {
    if(!valid()) {
        throw Exception("Calling Event member function on an invalid Event object");
    }
    // forward the call to the datastore's store function
    return m_impl->m_datastore->m_impl->store(0, m_impl->fullpath(), key, value, vsize);
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
    return m_impl->m_datastore->m_impl->load(0, m_impl->fullpath(), key, buffer);
}

bool Event::loadRawData(const std::string& key, char* value, size_t* vsize) const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    // forward the call to the datastore's load function
    return m_impl->m_datastore->m_impl->load(0, m_impl->fullpath(), key, value, vsize);
}

bool Event::operator==(const Event& other) const {
    bool v1 = valid();
    bool v2 = other.valid();
    if(!v1 && !v2) return true;
    if(!v1 &&  v2) return false;
    if(v1  && !v2) return false;
    return m_impl->m_datastore  == other.m_impl->m_datastore
        && m_impl->m_level      == other.m_impl->m_level
        && *m_impl->m_container == *other.m_impl->m_container
        && m_impl->m_event_nr   == other.m_impl->m_event_nr;
}

bool Event::operator!=(const Event& other) const {
    return !(*this == other);
}

const EventNumber& Event::number() const {
    if(!valid()) {
        throw Exception("Calling Event member function on an invalid Event object");
    }
    return m_impl->m_event_nr;
}

}
