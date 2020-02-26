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

EventSet::iterator EventSet::find(const RunNumber& runNumber,
                                  const SubRunNumber& subrunNumber,
                                  const EventNumber& eventNumber) {
    int ret;
    auto& datastore = m_impl->m_datastore;
    bool b = datastore->itemExists(m_impl->m_uuid, runNumber, subrunNumber, eventNumber);
    if(!b) return end();
    return iterator(
            std::make_shared<ItemImpl>(
                datastore,
                m_impl->m_uuid,
                runNumber));
}

EventSet::const_iterator EventSet::find(const RunNumber& runNumber,
                                        const SubRunNumber& subrunNumber,
                                        const EventNumber& eventNumber) const {
    iterator it = const_cast<EventSet*>(this)->find(runNumber, subrunNumber, eventNumber);
    return it;
}

EventSet::iterator EventSet::begin() {
    auto it = find(0,0,0);
    if(it != end()) return *it;

    auto ds_level = m_impl->m_level;
    auto datastore = m_impl->m_datastore;
    auto new_event_impl = std::make_shared<ItemImpl>(datastore, m_impl->m_uuid, 0, 0, 0);
    Event event(std::move(new_event_impl));
    event = event.next();

    if(event.valid()) return iterator(event);
    else return end();
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

EventSet::iterator EventSet::lower_bound(const RunNumber& lb_run,
                                         const SubRunNumber& lb_subrun,
                                         const EventNumber& lb_event) {
    if(lb_run == 0 && lb_subrun == 0 && lb_event == 0) {
        auto it = find(0,0,0);
        if(it != end()) {
            return it;
        } else {
            Event event(std::make_shared<ItemImpl>(
                    m_impl->m_datastore, 
                    m_impl->m_uuid, 0, 0, 0));
            event = event.next();
            if(!event.valid()) return end();
            else return iterator(event);
        }
    } else {
        // XXX the bellow is actually not correct
        auto it = find(lb_run, lb_subrun, lb_event-1);
        if(it != end()) {
            ++it;
            return it;
        }
        Event event(std::make_shared<ItemImpl>(
                m_impl->m_datastore, 
                m_impl->m_uuid, lb_event-1));
        event = event.next();
        if(!event.valid()) return end();
        else return iterator(event);
    }
}

EventSet::const_iterator EventSet::lower_bound(const RunNumber& lb_run,
                                               const SubRunNumber& lb_subrun,
                                               const EventNumber& lb_event) const {
    iterator it = const_cast<EventSet*>(this)->lower_bound(lb_run, lb_subrun, lb_event);
    return it;
}

EventSet::iterator EventSet::upper_bound(const RunNumber& ub_run,
                                         const SubRunNumber& ub_subrun,
                                         const EventNumber& ub_event) {
    Event event(std::make_shared<ItemImpl>(m_impl->m_datastore, 
                    m_impl->m_uuid, ub_run, ub_subrun, ub_event));
    event = event.next();
    if(!event.valid()) return end();
    else return iterator(event);
}

EventSet::const_iterator EventSet::upper_bound(const RunNumber& ub_run,
                                               const SubRunNumber& ub_subrun,
                                               const EventNumber& ub_event) const {
    iterator it = const_cast<EventSet*>(this)->upper_bound(ub_run, ub_subrun, ub_event);
    return it;
}

////////////////////////////////////////////////////////////////////////////////////////////
// EventSet::const_iterator::Impl implementation
////////////////////////////////////////////////////////////////////////////////////////////

class EventSet::const_iterator::Impl {
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
// EventSet::const_iterator implementation
////////////////////////////////////////////////////////////////////////////////////////////

EventSet::const_iterator::const_iterator()
: m_impl(std::make_unique<Impl>()) {}

EventSet::const_iterator::const_iterator(const Event& event)
: m_impl(std::make_unique<Impl>(event)) {}

EventSet::const_iterator::const_iterator(Event&& event)
: m_impl(std::make_unique<Impl>(std::move(event))) {}

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
    m_impl->m_current_event = m_impl->m_current_event.next();
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

EventSet::iterator::iterator(const Event& current)
: const_iterator(current) {}

EventSet::iterator::iterator(Event&& current)
: const_iterator(std::move(current)) {}

EventSet::iterator::iterator()
: const_iterator() {}

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

}
