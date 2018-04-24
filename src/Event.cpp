#include "hepnos/Event.hpp"
#include "private/EventImpl.hpp"
#include "private/DataStoreImpl.hpp"

namespace hepnos {

Event::Event()
: m_impl(std::make_unique<Impl>(nullptr, 0, "", 0)) {} 

Event::Event(DataStore* ds, uint8_t level, const std::string& container, const EventNumber& rn)
: m_impl(std::make_unique<Impl>(ds, level, container, rn)) { }

Event::Event(const Event& other)
: m_impl(std::make_unique<Impl>(*other.m_impl)) {}

Event::Event(Event&&) = default;

Event& Event::operator=(const Event& other) {
    if(this == &other) return *this;
    m_impl = std::make_unique<Impl>(*other.m_impl);
    return *this;
}

Event& Event::operator=(Event&&) = default;

Event::~Event() = default; 

Event Event::next() const {
    if(!valid()) return Event();
   
    std::vector<std::string> keys;
    size_t s = m_impl->m_datastore->m_impl->nextKeys(
            m_impl->m_level, m_impl->m_container, 
            m_impl->makeKeyStringFromEventNumber(), keys, 1);
    if(s == 0) return Event();
    size_t i = m_impl->m_container.size()+1;
    if(keys[0].size() <= i) return Event();
    if(keys[0][i] != '%') return Event();
    std::stringstream strEventNumber;
    strEventNumber << std::hex << keys[0].substr(i+1);
    EventNumber n;
    strEventNumber >> n;

    return Event(m_impl->m_datastore, m_impl->m_level, m_impl->m_container, n);
}

bool Event::valid() const {
    return m_impl && m_impl->m_datastore; 

}

bool Event::storeRawData(const std::string& key, const std::vector<char>& buffer) {
    if(!valid()) {
        throw Exception("Calling store() on invalid Event");
    }
    // forward the call to the datastore's store function
    return m_impl->m_datastore->m_impl->store(0, m_impl->fullpath(), key, buffer);
}

bool Event::loadRawData(const std::string& key, std::vector<char>& buffer) const {
    if(!valid()) {
        throw Exception("Calling load() on invalid Event");
    }
    // forward the call to the datastore's load function
    return m_impl->m_datastore->m_impl->load(0, m_impl->fullpath(), key, buffer);
}

bool Event::operator==(const Event& other) const {
    return m_impl->m_datastore == other.m_impl->m_datastore
        && m_impl->m_level     == other.m_impl->m_level
        && m_impl->m_container == other.m_impl->m_container
        && m_impl->m_event_nr    == other.m_impl->m_event_nr;
}

bool Event::operator!=(const Event& other) const {
    return !(*this == other);
}

const EventNumber& Event::number() const {
    return m_impl->m_event_nr;
}

}
