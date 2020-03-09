/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <memory>
#include "hepnos/SubRun.hpp"
#include "hepnos/AsyncEngine.hpp"
#include "ItemImpl.hpp"
#include "DataStoreImpl.hpp"
#include "WriteBatchImpl.hpp"
#include "AsyncEngineImpl.hpp"

namespace hepnos {

static SubRun::iterator SubRun_end;

SubRun::SubRun()
: m_impl(std::make_shared<ItemImpl>(nullptr, UUID(), InvalidRunNumber)) {} 

SubRun::SubRun(std::shared_ptr<ItemImpl>&& impl)
: m_impl(std::move(impl)) { }

SubRun::SubRun(const std::shared_ptr<ItemImpl>& impl)
: m_impl(impl) { }

DataStore SubRun::datastore() const {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    return DataStore(m_impl->m_datastore);
}

Run SubRun::run() const {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    ItemDescriptor run_descriptor(
            m_impl->m_descriptor.dataset,
            m_impl->m_descriptor.run);
    return Run(std::make_shared<ItemImpl>(m_impl->m_datastore, run_descriptor));
}

SubRun SubRun::next() const {
    if(!valid()) return SubRun();
  
    std::vector<std::shared_ptr<ItemImpl>> next_subruns; 
    size_t s = m_impl->m_datastore->nextItems(ItemType::SUBRUN, ItemType::RUN, m_impl, next_subruns, 1);
    if(s == 0) return SubRun();
    return SubRun(std::move(next_subruns[0]));
}

bool SubRun::valid() const {
    return m_impl && m_impl->m_datastore; 
}

ProductID SubRun::storeRawData(const std::string& key, const char* value, size_t vsize) {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    // forward the call to the datastore's store function
    auto& id = m_impl->m_descriptor;
    return m_impl->m_datastore->storeRawProduct(id, key, value, vsize);
}

ProductID SubRun::storeRawData(WriteBatch& batch, const std::string& key, const char* value, size_t vsize) {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    // forward the call to the batch's store function
    auto& id = m_impl->m_descriptor;
    if(batch.m_impl)
        return batch.m_impl->storeRawProduct(id, key, value, vsize);
    else
        return m_impl->m_datastore->storeRawProduct(id, key, value, vsize);
}

ProductID SubRun::storeRawData(AsyncEngine& async, const std::string& key, const char* value, size_t vsize) {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    // forward the call to async engine's store function
    auto& id = m_impl->m_descriptor;
    if(async.m_impl)
        return async.m_impl->storeRawProduct(id, key, value, vsize);
    else
        return m_impl->m_datastore->storeRawProduct(id, key, value, vsize);
}

bool SubRun::loadRawData(const std::string& key, std::string& buffer) const {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    auto& id = m_impl->m_descriptor;
    // forward the call to the datastore's load function
    return m_impl->m_datastore->loadRawProduct(id, key, buffer);
}

bool SubRun::loadRawData(const std::string& key, char* value, size_t* vsize) const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    // forward the call to the datastore's load function
    auto& id = m_impl->m_descriptor;
    return m_impl->m_datastore->loadRawProduct(id, key, value, vsize);
}

bool SubRun::operator==(const SubRun& other) const {
    bool v1 = valid();
    bool v2 = other.valid();
    if(!v1 && !v2) return true;
    if(!v1 &&  v2) return false;
    if(v1  && !v2) return false;
    return (m_impl == other.m_impl) || (*m_impl == *other.m_impl);
}

bool SubRun::operator!=(const SubRun& other) const {
    return !(*this == other);
}

const SubRunNumber& SubRun::number() const {
    return m_impl->m_descriptor.subrun;
}

Event SubRun::createEvent(const EventNumber& eventNumber) {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    auto& id = m_impl->m_descriptor;
    m_impl->m_datastore->createItem(id.dataset, id.run, id.subrun, eventNumber);
    return Event(std::make_shared<ItemImpl>(m_impl->m_datastore, id.dataset, id.run, id.subrun, eventNumber));
}

Event SubRun::createEvent(WriteBatch& batch, const EventNumber& eventNumber) {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    auto& id = m_impl->m_descriptor;
    if(batch.m_impl)
        batch.m_impl->createItem(id.dataset, id.run, id.subrun, eventNumber);
    else
        m_impl->m_datastore->createItem(id.dataset, id.run, id.subrun, eventNumber);
    return Event(std::make_shared<ItemImpl>(m_impl->m_datastore, id.dataset, id.run, id.subrun, eventNumber));
}

Event SubRun::createEvent(AsyncEngine& async, const EventNumber& eventNumber) {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    auto& id = m_impl->m_descriptor;
    if(async.m_impl)
        async.m_impl->createItem(id.dataset, id.run, id.subrun, eventNumber);
    else
        m_impl->m_datastore->createItem(id.dataset, id.run, id.subrun, eventNumber);
    return Event(std::make_shared<ItemImpl>(m_impl->m_datastore, id.dataset, id.run, id.subrun, eventNumber));
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
    auto& id = m_impl->m_descriptor;
    bool b = m_impl->m_datastore->itemExists(id.dataset, id.run, id.subrun, eventNumber);
    if(!b) {
        return SubRun_end;
    }
    return iterator(Event(std::make_shared<ItemImpl>(m_impl->m_datastore, id.dataset, id.run, id.subrun, eventNumber)));
}

SubRun::const_iterator SubRun::find(const EventNumber& eventNumber) const {
    iterator it = const_cast<SubRun*>(this)->find(eventNumber);
    return it;
}

SubRun::iterator SubRun::begin() {
    auto it = find(0);
    if(it != end()) return it;

    auto& datastore = m_impl->m_datastore;
    auto& id = m_impl->m_descriptor;
    Event event(std::make_shared<ItemImpl>(datastore, id.dataset, id.run, id.subrun, 0));
    event = event.next();

    if(event.valid()) return iterator(std::move(event));
    else return end();
}

SubRun::iterator SubRun::end() {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    return SubRun_end;
}

SubRun::const_iterator SubRun::begin() const {
    return const_iterator(const_cast<SubRun*>(this)->begin());
}

SubRun::const_iterator SubRun::end() const {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    return SubRun_end;
}

SubRun::const_iterator SubRun::cbegin() const {
    return const_iterator(const_cast<SubRun*>(this)->begin());
}

SubRun::const_iterator SubRun::cend() const {
    if(!valid()) {
        throw Exception("Calling SubRun member function on invalid SubRun object");
    }
    return SubRun_end;
}

SubRun::iterator SubRun::lower_bound(const EventNumber& lb) {
    if(lb == 0) {
        auto it = find(0);
        if(it != end()) {
            return it;
        } else {
            auto& id = m_impl->m_descriptor;
            Event event(std::make_shared<ItemImpl>(m_impl->m_datastore, id.dataset, id.run, id.subrun, 0));
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
        auto& id = m_impl->m_descriptor;
        Event event(std::make_shared<ItemImpl>(m_impl->m_datastore, id.dataset, id.run, id.subrun, lb-1));
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
    auto& id = m_impl->m_descriptor;
    Event event(std::make_shared<ItemImpl>(m_impl->m_datastore, id.dataset, id.run, id.subrun, ub));
    event = event.next();
    if(!event.valid()) return end();
    else return iterator(event);
}

SubRun::const_iterator SubRun::upper_bound(const EventNumber& ub) const {
    iterator it = const_cast<SubRun*>(this)->upper_bound(ub);
    return it;
}

void SubRun::toDescriptor(SubRunDescriptor& descriptor) {
    std::memset(descriptor.data, 0, sizeof(descriptor.data));
    if(!valid()) return;
    std::memcpy(descriptor.data, &(m_impl->m_descriptor), sizeof(descriptor.data));
}

SubRun SubRun::fromDescriptor(const DataStore& datastore, const SubRunDescriptor& descriptor, bool validate) {
    auto itemImpl = std::make_shared<ItemImpl>(datastore.m_impl, UUID(), InvalidRunNumber);
    auto& itemDescriptor = itemImpl->m_descriptor;
    std::memcpy(&itemDescriptor, descriptor.data, sizeof(descriptor.data));
    if((!validate) || datastore.m_impl->itemExists(itemDescriptor))
        return SubRun(std::move(itemImpl));
    else return SubRun();
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
