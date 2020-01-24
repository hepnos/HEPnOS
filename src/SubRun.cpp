/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <memory>
#include "hepnos/SubRun.hpp"
#include "private/SubRunImpl.hpp"
#include "private/EventImpl.hpp"
#include "private/DataStoreImpl.hpp"
#include "private/WriteBatchImpl.hpp"

namespace hepnos {

SubRun::SubRun()
: m_impl(std::make_unique<Impl>(nullptr, 0, std::make_shared<std::string>(""), InvalidSubRunNumber)) {} 

SubRun::SubRun(DataStore* ds, uint8_t level, const std::shared_ptr<std::string>& container, const SubRunNumber& rn)
: m_impl(std::make_unique<Impl>(ds, level, container, rn)) { }

SubRun::SubRun(const SubRun& other) {   
    if(other.m_impl)
        m_impl = std::make_unique<Impl>(*other.m_impl);
}

SubRun::SubRun(SubRun&&) = default;

SubRun& SubRun::operator=(const SubRun& other) {
    if(this == &other) return *this;
    if(other.m_impl)
        m_impl = std::make_unique<Impl>(*other.m_impl);
    else
        m_impl.reset();
    return *this;
}

SubRun& SubRun::operator=(SubRun&&) = default;

SubRun::~SubRun() = default; 

DataStore* SubRun::getDataStore() const {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    return m_impl->m_datastore;
}

SubRun SubRun::next() const {
    if(!valid()) return SubRun();
   
    std::vector<std::string> keys;
    size_t s = m_impl->m_datastore->m_impl->nextKeys(
            m_impl->m_level, *m_impl->m_container, 
            m_impl->makeKeyStringFromSubRunNumber(), keys, 1);
    if(s == 0) return SubRun();
    size_t i = m_impl->m_container->size()+1;
    if(keys[0].size() <= i) return SubRun();
    SubRunNumber rn = Impl::parseSubRunNumberFromKeyString(&keys[0][i]);
    if(rn == InvalidSubRunNumber) return SubRun();
    return SubRun(m_impl->m_datastore, m_impl->m_level, m_impl->m_container, rn);
}

bool SubRun::valid() const {
    return m_impl && m_impl->m_datastore; 
}

ProductID SubRun::storeRawData(const std::string& key, const std::string& buffer) {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    // forward the call to the datastore's store function
    return m_impl->m_datastore->m_impl->store(0, m_impl->fullpath(), key, buffer);
}

ProductID SubRun::storeRawData(std::string&& key, std::string&& buffer) {
    return storeRawData(key, buffer); // call above function
}

ProductID SubRun::storeRawData(WriteBatch& batch, const std::string& key, const std::string& buffer) {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    // forward the call to the datastore's store function
    return batch.m_impl->store(0, m_impl->fullpath(), key, buffer);
}

ProductID SubRun::storeRawData(WriteBatch& batch, std::string&& key, std::string&& buffer) {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    // forward the call to the datastore's store function
    return batch.m_impl->store(0, m_impl->fullpath(), std::move(key), std::move(buffer));
}

bool SubRun::loadRawData(const std::string& key, std::string& buffer) const {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    // forward the call to the datastore's load function
    return m_impl->m_datastore->m_impl->load(0, m_impl->fullpath(), key, buffer);
}

bool SubRun::operator==(const SubRun& other) const {
    bool v1 = valid();
    bool v2 = other.valid();
    if(!v1 && !v2) return true;
    if(!v1 &&  v2) return false;
    if(v1  && !v2) return false;
    return m_impl->m_datastore == other.m_impl->m_datastore
        && m_impl->m_level     == other.m_impl->m_level
        && m_impl->m_container == other.m_impl->m_container
        && m_impl->m_subrun_nr    == other.m_impl->m_subrun_nr;
}

bool SubRun::operator!=(const SubRun& other) const {
    return !(*this == other);
}

const SubRunNumber& SubRun::number() const {
    return m_impl->m_subrun_nr;
}

Event SubRun::createEvent(const EventNumber& eventNumber) {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    std::string parent = m_impl->fullpath();
    std::string eventStr = Event::Impl::makeKeyStringFromEventNumber(eventNumber);
    m_impl->m_datastore->m_impl->store(m_impl->m_level+1, parent, eventStr, std::string());
    return Event(m_impl->m_datastore, m_impl->m_level+1,
            std::make_shared<std::string>(parent), eventNumber);
}

Event SubRun::createEvent(WriteBatch& batch, const EventNumber& eventNumber) {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    std::string parent = m_impl->fullpath();
    std::string eventStr = Event::Impl::makeKeyStringFromEventNumber(eventNumber);
    batch.m_impl->store(m_impl->m_level+1, parent, eventStr, std::string());
    return Event(m_impl->m_datastore, m_impl->m_level+1,
            std::make_shared<std::string>(parent), eventNumber);
}

Event SubRun::operator[](const EventNumber& eventNumber) const {
    auto it = find(eventNumber);
    if(!it->valid())
        throw Exception("Requested Event does not exist");
    return std::move(*it);
}

SubRun::iterator SubRun::find(const EventNumber& eventNumber) {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    int ret;
    std::string parent = m_impl->fullpath();
    std::string eventStr = Event::Impl::makeKeyStringFromEventNumber(eventNumber);
    bool b = m_impl->m_datastore->m_impl->exists(m_impl->m_level+1, parent, eventStr);
    if(!b) {
        return m_impl->m_end;
    }
    return iterator(Event(m_impl->m_datastore, m_impl->m_level+1,
                std::make_shared<std::string>(parent), eventNumber));
}

SubRun::const_iterator SubRun::find(const EventNumber& eventNumber) const {
    iterator it = const_cast<SubRun*>(this)->find(eventNumber);
    return it;
}

SubRun::iterator SubRun::begin() {
    auto it = find(0);
    if(it != end()) return it;

    auto level = m_impl->m_level;
    auto datastore = m_impl->m_datastore;
    std::string container = m_impl->fullpath();
    Event event(datastore, level+1, std::make_shared<std::string>(container), 0);
    event = event.next();

    if(event.valid()) return iterator(std::move(event));
    else return end();
}

SubRun::iterator SubRun::end() {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    return m_impl->m_end;
}

SubRun::const_iterator SubRun::begin() const {
    return const_iterator(const_cast<SubRun*>(this)->begin());
}

SubRun::const_iterator SubRun::end() const {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    return m_impl->m_end;
}

SubRun::const_iterator SubRun::cbegin() const {
    return const_iterator(const_cast<SubRun*>(this)->begin());
}

SubRun::const_iterator SubRun::cend() const {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    return m_impl->m_end;
}

SubRun::iterator SubRun::lower_bound(const EventNumber& lb) {
    if(lb == 0) {
        auto it = find(0);
        if(it != end()) {
            return it;
        } else {
            Event event(m_impl->m_datastore, 
                    m_impl->m_level+1,
                    std::make_shared<std::string>(m_impl->fullpath()), 0);
            event = event.next();
            if(!event.valid()) return end();
            else return iterator(event);
        }
    } else {
        auto it = find(lb-1);
        if(it != end()) {
            ++it;
            return it;
        }
        Event event(m_impl->m_datastore,
                m_impl->m_level+1,
                std::make_shared<std::string>(m_impl->fullpath()), lb-1);
        event = event.next();
        if(!event.valid()) return end();
        else return iterator(event);
    }
}

SubRun::const_iterator SubRun::lower_bound(const EventNumber& lb) const {
    iterator it = const_cast<SubRun*>(this)->lower_bound(lb);
    return it;
}

SubRun::iterator SubRun::upper_bound(const EventNumber& ub) {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    Event event(m_impl->m_datastore, 
            m_impl->m_level+1, 
            std::make_shared<std::string>(m_impl->fullpath()), ub);
    event = event.next();
    if(!event.valid()) return end();
    else return iterator(event);
}

SubRun::const_iterator SubRun::upper_bound(const EventNumber& ub) const {
    iterator it = const_cast<SubRun*>(this)->upper_bound(ub);
    return it;
}

////////////////////////////////////////////////////////////////////////////////////////////
// SubRun::const_iterator::Impl implementation
////////////////////////////////////////////////////////////////////////////////////////////

class SubRun::const_iterator::Impl {
    public:
        Event m_current_event;

        Impl()
        : m_current_event()
        {}

        Impl(const Event& event)
        : m_current_event(event)
        {}

        Impl(Event&& event)
            : m_current_event(std::move(event))
        {}

        Impl(const Impl& other)
            : m_current_event(other.m_current_event)
        {}

        bool operator==(const Impl& other) const {
            return m_current_event == other.m_current_event;
        }
};

////////////////////////////////////////////////////////////////////////////////////////////
// SubRun::const_iterator implementation
////////////////////////////////////////////////////////////////////////////////////////////

SubRun::const_iterator::const_iterator()
: m_impl(std::make_unique<Impl>()) {}

SubRun::const_iterator::const_iterator(const Event& event)
: m_impl(std::make_unique<Impl>(event)) {}

SubRun::const_iterator::const_iterator(Event&& event)
: m_impl(std::make_unique<Impl>(std::move(event))) {}

SubRun::const_iterator::~const_iterator() {}

SubRun::const_iterator::const_iterator(const SubRun::const_iterator& other) {
    if(other.m_impl)
        m_impl = std::make_unique<Impl>(*other.m_impl);
}

SubRun::const_iterator::const_iterator(SubRun::const_iterator&& other)
: m_impl(std::move(other.m_impl)) {}

SubRun::const_iterator& SubRun::const_iterator::operator=(const SubRun::const_iterator& other) {
    if(&other == this) return *this;
    if(other.m_impl)
        m_impl = std::make_unique<Impl>(*other.m_impl);
    else
        m_impl.reset();
    return *this;
}

SubRun::const_iterator& SubRun::const_iterator::operator=(SubRun::const_iterator&& other) {
    if(&other == this) return *this;
    m_impl = std::move(other.m_impl);
    return *this;
}

SubRun::const_iterator::self_type SubRun::const_iterator::operator++() {
    if(!m_impl) {
        throw Exception("Trying to increment an invalid iterator");
    }
    m_impl->m_current_event = m_impl->m_current_event.next();
    return *this;
}

SubRun::const_iterator::self_type SubRun::const_iterator::operator++(int) {
    const_iterator copy = *this;
    ++(*this);
    return copy;
}

const SubRun::const_iterator::reference SubRun::const_iterator::operator*() {
    if(!m_impl) {
        throw Exception("Trying to dereference an invalid iterator");
    }
    return m_impl->m_current_event;
}

const SubRun::const_iterator::pointer SubRun::const_iterator::operator->() {
    if(!m_impl) {
        throw Exception("Trying to dereference an invalid iterator");
    }
    return &(m_impl->m_current_event);
}

bool SubRun::const_iterator::operator==(const self_type& rhs) const {
    if(!m_impl && !rhs.m_impl)  return true;
    if(m_impl  && !rhs.m_impl)  return false;
    if(!m_impl && rhs.m_impl)   return false;
    return *m_impl == *(rhs.m_impl);
}

bool SubRun::const_iterator::operator!=(const self_type& rhs) const {
        return !(*this == rhs);
}

////////////////////////////////////////////////////////////////////////////////////////////
// SubRun::iterator implementation
////////////////////////////////////////////////////////////////////////////////////////////

SubRun::iterator::iterator(const Event& current)
: const_iterator(current) {}

SubRun::iterator::iterator(Event&& current)
: const_iterator(std::move(current)) {}

SubRun::iterator::iterator()
: const_iterator() {}

SubRun::iterator::~iterator() {}

SubRun::iterator::iterator(const SubRun::iterator& other)
: const_iterator(other) {}

SubRun::iterator::iterator(SubRun::iterator&& other)
: const_iterator(std::move(other)) {}

SubRun::iterator& SubRun::iterator::operator=(const SubRun::iterator& other) {
    if(this == &other) return *this;
    if(other.m_impl)
        m_impl = std::make_unique<Impl>(*other.m_impl);
    else
        m_impl.reset();
    return *this;
}

SubRun::iterator& SubRun::iterator::operator=(SubRun::iterator&& other) {
    if(this == &other) return *this;
    m_impl = std::move(other.m_impl);
    return *this;
}

SubRun::iterator::reference SubRun::iterator::operator*() {
    return const_cast<reference>(const_iterator::operator*());
}

SubRun::iterator::pointer SubRun::iterator::operator->() {
    return const_cast<pointer>(const_iterator::operator->());
}

}
