/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include <iomanip>
#include <sstream>
#include <string>
#include "hepnos/DataSet.hpp"
#include "hepnos/EventSet.hpp"
#include "EventSetImpl.hpp"
#include "DataStoreImpl.hpp"
#include "ItemImpl.hpp"

namespace hepnos {

////////////////////////////////////////////////////////////////////////////////////////////
// EventSet::const_iterator::Impl implementation
////////////////////////////////////////////////////////////////////////////////////////////

class EventSet::const_iterator::Impl {

        friend class EventSet;

    public:
        Event m_current_event;
        int m_target = 0;
        int m_num_targets = 0;

        Impl()
        : m_current_event()
        {}

        Impl(const Event& event, int target, int num_targets=0)
        : m_current_event(event)
        , m_target(target)
        , m_num_targets(num_targets)
        {}

        Impl(Event&& event, int target, int num_targets=0)
        : m_current_event(std::move(event))
        , m_target(target)
        , m_num_targets(num_targets)
        {}

        Impl(const Impl& other)
        : m_current_event(other.m_current_event)
        , m_target(other.m_target)
        , m_num_targets(other.m_num_targets)
        {}

        bool operator==(const Impl& other) const {
            auto v1 = m_current_event.valid();
            auto v2 = other.m_current_event.valid();
            if(!v1 && !v2) return true;
            if(!v1 &&  v2) return false;
            if(v1  && !v2) return false;
            return m_current_event == other.m_current_event
                && m_target == other.m_target
                && m_num_targets == other.m_num_targets;
        }
};

////////////////////////////////////////////////////////////////////////////////////////////
// EventSet::const_iterator implementation
////////////////////////////////////////////////////////////////////////////////////////////

EventSet::const_iterator::const_iterator()
: m_impl(std::make_unique<Impl>()) {}

EventSet::const_iterator::const_iterator(std::unique_ptr<Impl>&& impl)
: m_impl(std::move(impl)) {}

EventSet::const_iterator::~const_iterator() {}

EventSet::const_iterator::const_iterator(const EventSet::const_iterator& other)
: m_impl(std::make_unique<Impl>(*other.m_impl)) {}

EventSet::const_iterator::const_iterator(EventSet::const_iterator&& other)
: m_impl(std::move(other.m_impl)) {}

EventSet::const_iterator& EventSet::const_iterator::operator=(const EventSet::const_iterator& other) {
    if(&other == this) return *this;
    m_impl = std::make_unique<Impl>(*other.m_impl);
    return *this;
}

EventSet::const_iterator& EventSet::const_iterator::operator=(EventSet::const_iterator&& other) {
    if(&other == this) return *this;
    m_impl = std::move(other.m_impl);
    return *this;
}

EventSet::const_iterator::self_type EventSet::const_iterator::operator++() {
    if(!m_impl) {
        throw Exception("Trying to increment an invalid iterator");
    }
    if(!(m_impl->m_current_event.valid())) {
        throw Exception("Trying to increment an iterator past the end of the EventSet");
    }
    std::vector<std::shared_ptr<ItemImpl>> next_events;
    auto& ds = m_impl->m_current_event.m_impl->m_datastore;

    if(m_impl->m_num_targets == 0) { // single target access
        size_t s = ds->nextItems(ItemType::EVENT, ItemType::DATASET,
                                 m_impl->m_current_event.m_impl,
                                 next_events, 1, m_impl->m_target);
        if(s != 0) {
            m_impl->m_current_event.m_impl = std::move(next_events[0]);
        } else {
            m_impl->m_current_event = Event();
        }
    } else { // multi-target access
        // try to get next event from current target
        size_t s = ds->nextItems(ItemType::EVENT, ItemType::DATASET,
                                 m_impl->m_current_event.m_impl,
                                 next_events, 1, m_impl->m_target);
        if(s == 1) {
            // event found
            m_impl->m_current_event.m_impl = std::move(next_events[0]);
            return *this;
        }
        // event not found, incrementing target
        m_impl->m_target += 1;
        // start looping over targets
        while(m_impl->m_target < m_impl->m_num_targets) {
            // search for event (0,0,0)
            auto& uuid = m_impl->m_current_event.m_impl->m_descriptor.dataset;
            auto event_impl000 = std::make_shared<ItemImpl>(ds, uuid, 0, 0, 0);
            if(ds->itemExists(uuid, 0, 0, 0, m_impl->m_target)) {
                // there exists an item 0,0,0; make the iterator point to it
                m_impl->m_current_event.m_impl = std::move(event_impl000);
                return *this;
            } 
            // if event (0,0,0) does not exist, then try to get the next one
            size_t s = ds->nextItems(ItemType::EVENT, ItemType::DATASET,
                    event_impl000, next_events, 1, m_impl->m_target);
            if(s == 1) {
                // item found, make the iterator point to it
                m_impl->m_current_event.m_impl = std::move(next_events[0]);
                return *this;
            }
            m_impl->m_target += 1;
        }
        // if we get out of the loop, we haven't found any event
        m_impl->m_current_event = Event();
    }
    return *this;
}

EventSet::const_iterator::self_type EventSet::const_iterator::operator++(int) {
    const_iterator copy = *this;
    ++(*this);
    return copy;
}

const EventSet::const_iterator::reference EventSet::const_iterator::operator*() {
    if(!m_impl) {
        throw Exception("Trying to dereference an invalid iterator");
    }
    return m_impl->m_current_event;
}

const EventSet::const_iterator::pointer EventSet::const_iterator::operator->() {
    if(!m_impl) return nullptr;
    return &(m_impl->m_current_event);
}

bool EventSet::const_iterator::operator==(const self_type& rhs) const {
    if(!m_impl && !rhs.m_impl)  return true;
    if(m_impl  && !rhs.m_impl)  return false;
    if(!m_impl && rhs.m_impl)   return false;
    return *m_impl == *(rhs.m_impl);
}

bool EventSet::const_iterator::operator!=(const self_type& rhs) const {
        return !(*this == rhs);
}

////////////////////////////////////////////////////////////////////////////////////////////
// EventSet::iterator implementation
////////////////////////////////////////////////////////////////////////////////////////////

EventSet::iterator::iterator()
: const_iterator() {}

EventSet::iterator::iterator(std::unique_ptr<EventSet::const_iterator::Impl>&& impl)
: const_iterator(std::move(impl)) {}

EventSet::iterator::~iterator() {}

EventSet::iterator::iterator(const EventSet::iterator& other)
: const_iterator(other) {}

EventSet::iterator::iterator(EventSet::iterator&& other)
: const_iterator(std::move(other)) {}

EventSet::iterator& EventSet::iterator::operator=(const EventSet::iterator& other) {
    if(this == &other) return *this;
    m_impl = std::make_unique<Impl>(*other.m_impl);
    return *this;
}

EventSet::iterator& EventSet::iterator::operator=(EventSet::iterator&& other) {
    if(this == &other) return *this;
    m_impl = std::move(other.m_impl);
    return *this;
}

EventSet::iterator::reference EventSet::iterator::operator*() {
    return const_cast<reference>(const_iterator::operator*());
}

EventSet::iterator::pointer EventSet::iterator::operator->() {
    return const_cast<pointer>(const_iterator::operator->());
}

////////////////////////////////////////////////////////////////////////////////////////////
// EventSet implementation
////////////////////////////////////////////////////////////////////////////////////////////
static EventSet::iterator EventSet_end;

EventSet::EventSet(const std::shared_ptr<EventSetImpl>& impl)
: m_impl(impl) {}

EventSet::EventSet(std::shared_ptr<EventSetImpl>&& impl)
: m_impl(std::move(impl)) {}

DataStore EventSet::datastore() const {
    return DataStore(m_impl->m_datastore);
}

EventSet::iterator EventSet::begin() {
    // search for the first event in the event set
    auto& datastore = m_impl->m_datastore;
    // set the target at which to start
    int target = m_impl->m_target;
    if(target == -1) target = 0;
    int num_targets = m_impl->m_num_targets;
    do {
        // search for event (0,0,0)
        auto event_impl000 = std::make_shared<ItemImpl>(datastore, m_impl->m_uuid, 0, 0, 0);
        if(datastore->itemExists(m_impl->m_uuid, 0, 0, 0, target)) {
            // there exists an item 0,0,0; make the iterator point to it
            Event event(std::move(event_impl000));
            auto iterator_impl = std::unique_ptr<EventSet::const_iterator::Impl>(
                    new EventSet::const_iterator::Impl(
                        std::move(event), target, num_targets));
            return iterator(std::move(iterator_impl)); 
        }
        // if event (0,0,0) does not exist, then try to get the next one
        std::vector<std::shared_ptr<ItemImpl>> nextItems;
        size_t s = datastore->nextItems(ItemType::EVENT, ItemType::DATASET,
                                event_impl000, nextItems, 1, target);
        if(s == 1) {
            // item found, make the iterator point to it
            auto iterator_impl = std::unique_ptr<EventSet::const_iterator::Impl>(
                    new EventSet::const_iterator::Impl(
                        std::move(nextItems[0]), target, num_targets));
            return iterator(std::move(iterator_impl));
        }
        // if we haven't found any more items, and we can increase the target number,
        // let's do it and try with the next target
        target += 1;
    } while(target < num_targets);
    return end();
}

EventSet::iterator EventSet::end() {
    return EventSet_end;
}

EventSet::const_iterator EventSet::cbegin() const {
    return const_iterator(const_cast<EventSet*>(this)->begin());
}

EventSet::const_iterator EventSet::cend() const {
    return EventSet_end;
}

EventSet::const_iterator EventSet::begin() const {
    return const_iterator(const_cast<EventSet*>(this)->begin());
}

EventSet::const_iterator EventSet::end() const {
    return EventSet_end;
}


}
