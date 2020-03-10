/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "hepnos/Run.hpp"
#include "hepnos/DataSet.hpp"
#include "hepnos/AsyncEngine.hpp"
#include "hepnos/Prefetcher.hpp"
#include "ItemImpl.hpp"
#include "PrefetcherImpl.hpp"
#include "DataStoreImpl.hpp"
#include "WriteBatchImpl.hpp"
#include "AsyncEngineImpl.hpp"

namespace hepnos {

////////////////////////////////////////////////////////////////////////////////////////////
// Run::const_iterator::Impl declaration
////////////////////////////////////////////////////////////////////////////////////////////

class Run::const_iterator::Impl {

    friend class Run;

    public:
        SubRun m_current_subrun;
        std::shared_ptr<PrefetcherImpl> m_prefetcher;

        Impl()
        : m_current_subrun()
        {}

        Impl(const SubRun& subrun)
        : m_current_subrun(subrun)
        {}

        Impl(SubRun&& subrun)
            : m_current_subrun(std::move(subrun))
        {}

        Impl(const Impl& other)
            : m_current_subrun(other.m_current_subrun)
        {}

        ~Impl() {
            if(m_prefetcher)
                m_prefetcher->m_associated = false;
        }

        bool operator==(const Impl& other) const {
            return m_current_subrun == other.m_current_subrun;
        }

        void setPrefetcher(const std::shared_ptr<PrefetcherImpl>& p) {
            if(p->m_associated)
                throw Exception("Prefetcher object already in use");
            if(m_prefetcher)
                m_prefetcher->m_associated = false;
            m_prefetcher = p;
            m_prefetcher->m_associated = true;
        }
};


static Run::iterator Run_end;

Run::Run()
: m_impl(std::make_shared<ItemImpl>(nullptr, UUID(), InvalidRunNumber)) {} 

Run::Run(std::shared_ptr<ItemImpl>&& impl)
: m_impl(std::move(impl)) { }

Run::Run(const std::shared_ptr<ItemImpl>& impl)
: m_impl(impl) { }

DataStore Run::datastore() const {
    if(!valid()) { 
        throw Exception("Calling Run member function on an invalid Run object");
    }
    return DataStore(m_impl->m_datastore);
}

Run Run::next() const {
    if(!valid()) return Run();
   
    std::vector<std::shared_ptr<ItemImpl>> next_runs;
    size_t s = m_impl->m_datastore->nextItems(ItemType::RUN, ItemType::DATASET, m_impl, next_runs, 1);
    if(s == 0) return Run();
    return Run(std::move(next_runs[0]));
}

bool Run::valid() const {
    return m_impl && m_impl->m_datastore; 

}

ProductID Run::storeRawData(const std::string& key, const char* value, size_t vsize) {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    // forward the call to the datastore's store function
    const ItemDescriptor& id = m_impl->m_descriptor;
    return m_impl->m_datastore->storeRawProduct(id, key, value, vsize);
}

ProductID Run::storeRawData(WriteBatch& batch, const std::string& key, const char* value, size_t vsize) {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    // forward the call to the batch's store function
    const ItemDescriptor& id = m_impl->m_descriptor;
    if(batch.m_impl)
        return batch.m_impl->storeRawProduct(id, key, value, vsize);
    else
        return m_impl->m_datastore->storeRawProduct(id, key, value, vsize);
}

ProductID Run::storeRawData(AsyncEngine& async, const std::string& key, const char* value, size_t vsize) {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    // forward the call to the async engine's store function
    const ItemDescriptor& id = m_impl->m_descriptor;
    if(async.m_impl)
        return async.m_impl->storeRawProduct(id, key, value, vsize);
    else
        return m_impl->m_datastore->storeRawProduct(id, key, value, vsize);
}

bool Run::loadRawData(const std::string& key, std::string& buffer) const {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    // forward the call to the datastore's load function
    const ItemDescriptor& id = m_impl->m_descriptor;
    return m_impl->m_datastore->loadRawProduct(id, key, buffer);
}

bool Run::loadRawData(const std::string& key, char* value, size_t* vsize) const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    // forward the call to the datastore's load function
    const ItemDescriptor& id = m_impl->m_descriptor;
    return m_impl->m_datastore->loadRawProduct(id, key, value, vsize);
}

bool Run::operator==(const Run& other) const {
    bool v1 = valid();
    bool v2 = other.valid();
    if(!v1 && !v2) return true;
    if(v1 && !v2)  return false;
    if(!v1 && v2)  return false;
    return (m_impl == other.m_impl) || (*m_impl == *other.m_impl);
}

bool Run::operator!=(const Run& other) const {
    return !(*this == other);
}

const RunNumber& Run::number() const {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    return m_impl->m_descriptor.run;
}

SubRun Run::createSubRun(const SubRunNumber& subRunNumber) {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    ItemDescriptor& id = m_impl->m_descriptor;
    m_impl->m_datastore->createItem(id.dataset, id.run, subRunNumber);
    auto new_subrun_impl = std::make_shared<ItemImpl>(m_impl->m_datastore, id.dataset, id.run, subRunNumber);
    return SubRun(std::move(new_subrun_impl));
}

SubRun Run::createSubRun(WriteBatch& batch, const SubRunNumber& subRunNumber) {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    ItemDescriptor& id = m_impl->m_descriptor;
    if(batch.m_impl)
        batch.m_impl->createItem(id.dataset, id.run, subRunNumber);
    else
        m_impl->m_datastore->createItem(id.dataset, id.run, subRunNumber);
    auto new_subrun_impl = std::make_shared<ItemImpl>(m_impl->m_datastore, id.dataset, id.run, subRunNumber);
    return SubRun(std::move(new_subrun_impl));
}

SubRun Run::createSubRun(AsyncEngine& async, const SubRunNumber& subRunNumber) {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    ItemDescriptor& id = m_impl->m_descriptor;
    if(async.m_impl)
        async.m_impl->createItem(id.dataset, id.run, subRunNumber);
    else
        m_impl->m_datastore->createItem(id.dataset, id.run, subRunNumber);
    auto new_subrun_impl = std::make_shared<ItemImpl>(m_impl->m_datastore, id.dataset, id.run, subRunNumber);
    return SubRun(std::move(new_subrun_impl));
}

SubRun Run::operator[](const SubRunNumber& subRunNumber) const {
    auto it = find(subRunNumber);
    if(!it->valid())
        throw Exception("Requested SubRun does not exist");
    return std::move(*it);
}

Run::iterator Run::find(const SubRunNumber& subRunNumber) {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    auto& id = m_impl->m_descriptor;
    bool b = m_impl->m_datastore->itemExists(id.dataset, 
                                             id.run,
                                             subRunNumber);
    if(!b) {
        return Run_end;
    }
    auto new_subrun_impl = std::make_shared<ItemImpl>(m_impl->m_datastore, id.dataset, id.run, subRunNumber);
    return iterator(SubRun(std::move(new_subrun_impl)));
}

Run::iterator Run::find(const SubRunNumber& subRunNumber, const Prefetcher& prefetcher) {
    auto it = find(subRunNumber);
    if(it != end()) {
        it.m_impl->setPrefetcher(prefetcher.m_impl);
        prefetcher.m_impl->prefetchFrom(ItemType::SUBRUN, ItemType::RUN, it.m_impl->m_current_subrun.m_impl);
    }
    return it;
}

Run::const_iterator Run::find(const SubRunNumber& subRunNumber) const {
    iterator it = const_cast<Run*>(this)->find(subRunNumber);
    return it;
}

Run::iterator Run::begin() {
    auto it = find(0);
    if(it != end()) return it;

    auto& id = m_impl->m_descriptor;
    auto new_subrun_impl = std::make_shared<ItemImpl>(m_impl->m_datastore, id.dataset, id.run, 0);

    SubRun subrun(std::move(new_subrun_impl));
    subrun = subrun.next();

    if(subrun.valid()) return iterator(subrun);
    else return end();
}

Run::iterator Run::begin(const Prefetcher& prefetcher) {
    auto it = begin();
    if(it != end()) {
        it.m_impl->setPrefetcher(prefetcher.m_impl);
        prefetcher.m_impl->prefetchFrom(ItemType::SUBRUN, ItemType::RUN, it.m_impl->m_current_subrun.m_impl);
    }
    return it;
}

Run::iterator Run::end() {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    return Run_end;
}

Run::const_iterator Run::begin() const {
    return const_iterator(const_cast<Run*>(this)->begin());
}

Run::const_iterator Run::begin(const Prefetcher& prefetcher) const {
    return const_iterator(const_cast<Run*>(this)->begin(prefetcher));
}

Run::const_iterator Run::end() const {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    return Run_end;
}

Run::const_iterator Run::cbegin() const {
    return const_iterator(const_cast<Run*>(this)->begin());
}

Run::const_iterator Run::cbegin(const Prefetcher& prefetcher) const {
    return const_iterator(const_cast<Run*>(this)->begin(prefetcher));
}

Run::const_iterator Run::cend() const {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    return Run_end;
}

Run::iterator Run::lower_bound(const SubRunNumber& lb) {
    if(lb == 0) {
        auto it = find(0);
        if(it != end()) {
            return it;
        } else {
            auto& id = m_impl->m_descriptor;
            auto new_subrun_impl = std::make_shared<ItemImpl>(m_impl->m_datastore, id.dataset, id.run, 0);
            SubRun subrun(std::move(new_subrun_impl));
            subrun = subrun.next();
            if(!subrun.valid()) return end();
            else return iterator(subrun);
        }
    } else {
        auto it = find(lb-1);
        if(it != end()) {
            ++it;
            return it;
        }
        auto& id = m_impl->m_descriptor;
        auto new_subrun_impl = std::make_shared<ItemImpl>(m_impl->m_datastore, id.dataset, id.run, lb-1);
        SubRun subrun(std::move(new_subrun_impl));
        subrun = subrun.next();
        if(!subrun.valid()) return end();
        else return iterator(subrun);
    }
}

Run::iterator Run::lower_bound(const SubRunNumber& lb, const Prefetcher& prefetcher) {
    auto it = lower_bound(lb);
    if(it != end()) {
        it.m_impl->setPrefetcher(prefetcher.m_impl);
        prefetcher.m_impl->prefetchFrom(ItemType::SUBRUN, ItemType::RUN, it.m_impl->m_current_subrun.m_impl);
    }
    return it;
}

Run::const_iterator Run::lower_bound(const SubRunNumber& lb) const {
    return const_cast<Run*>(this)->lower_bound(lb);
}

Run::const_iterator Run::lower_bound(const SubRunNumber& lb, const Prefetcher& prefetcher) const {
    return const_cast<Run*>(this)->lower_bound(lb, prefetcher);
}

Run::iterator Run::upper_bound(const SubRunNumber& ub) {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    auto& id = m_impl->m_descriptor;
    auto new_subrun_impl = std::make_shared<ItemImpl>(m_impl->m_datastore, id.dataset, id.run, ub);
    SubRun subrun(std::move(new_subrun_impl));
    subrun = subrun.next();
    if(!subrun.valid()) return end();
    else return iterator(subrun);
}

Run::iterator Run::upper_bound(const SubRunNumber& ub, const Prefetcher& prefetcher) {
    auto it = upper_bound(ub);
    if(it != end()) {
        it.m_impl->setPrefetcher(prefetcher.m_impl);
        prefetcher.m_impl->prefetchFrom(ItemType::SUBRUN, ItemType::RUN, it.m_impl->m_current_subrun.m_impl);
    }
    return it;
}

Run::const_iterator Run::upper_bound(const SubRunNumber& ub) const {
    return const_cast<Run*>(this)->upper_bound(ub);
}

Run::const_iterator Run::upper_bound(const SubRunNumber& ub, const Prefetcher& prefetcher) const {
    return const_cast<Run*>(this)->upper_bound(ub, prefetcher);
}

void Run::toDescriptor(RunDescriptor& descriptor) {
    std::memset(descriptor.data, 0, sizeof(descriptor.data));
    if(!valid()) return;
    std::memcpy(descriptor.data, &(m_impl->m_descriptor), sizeof(descriptor.data));
}

Run Run::fromDescriptor(const DataStore& datastore, const RunDescriptor& descriptor, bool validate) {
    auto itemImpl = std::make_shared<ItemImpl>(datastore.m_impl, UUID(), InvalidRunNumber);
    auto& itemDescriptor = itemImpl->m_descriptor;
    std::memcpy(&itemDescriptor, descriptor.data, sizeof(descriptor.data));
    if((!validate) || datastore.m_impl->itemExists(itemDescriptor))
        return Run(std::move(itemImpl));
    else return Run();
}

////////////////////////////////////////////////////////////////////////////////////////////
// Run::const_iterator implementation
////////////////////////////////////////////////////////////////////////////////////////////

Run::const_iterator::const_iterator()
: m_impl(std::make_unique<Impl>()) {}

Run::const_iterator::const_iterator(const SubRun& subrun)
: m_impl(std::make_unique<Impl>(subrun)) {}

Run::const_iterator::const_iterator(SubRun&& subrun)
: m_impl(std::make_unique<Impl>(std::move(subrun))) {}

Run::const_iterator::~const_iterator() {}

Run::const_iterator::const_iterator(const Run::const_iterator& other) {
    if(other.m_impl) 
        m_impl = std::make_unique<Impl>(*other.m_impl);
}

Run::const_iterator::const_iterator(Run::const_iterator&& other)
: m_impl(std::move(other.m_impl)) {}

Run::const_iterator& Run::const_iterator::operator=(const Run::const_iterator& other) {
    if(&other == this) return *this;
    if(other.m_impl)
        m_impl = std::make_unique<Impl>(*other.m_impl);
    else
        m_impl.reset();
    return *this;
}

Run::const_iterator& Run::const_iterator::operator=(Run::const_iterator&& other) {
    if(&other == this) return *this;
    m_impl = std::move(other.m_impl);
    return *this;
}

Run::const_iterator::self_type Run::const_iterator::operator++() {
    if(!m_impl) {
        throw Exception("Trying to increment an invalid iterator");
    }
    m_impl->m_current_subrun = m_impl->m_current_subrun.next();
    return *this;
}

Run::const_iterator::self_type Run::const_iterator::operator++(int) {
    const_iterator copy = *this;
    ++(*this);
    return copy;
}

const Run::const_iterator::reference Run::const_iterator::operator*() {
    if(!m_impl) {
        throw Exception("Trying to dereference an invalid iterator");
    }
    return m_impl->m_current_subrun;
}

const Run::const_iterator::pointer Run::const_iterator::operator->() {
    if(!m_impl) return nullptr;
    return &(m_impl->m_current_subrun);
}

bool Run::const_iterator::operator==(const self_type& rhs) const {
    if(!m_impl && !rhs.m_impl)  return true;
    if(m_impl  && !rhs.m_impl)  return false;
    if(!m_impl && rhs.m_impl)   return false;
    return *m_impl == *(rhs.m_impl);
}

bool Run::const_iterator::operator!=(const self_type& rhs) const {
    return !(*this == rhs);
}

////////////////////////////////////////////////////////////////////////////////////////////
// Run::iterator implementation
////////////////////////////////////////////////////////////////////////////////////////////

Run::iterator::iterator(const SubRun& current)
: const_iterator(current) {}

Run::iterator::iterator(SubRun&& current)
: const_iterator(std::move(current)) {}

Run::iterator::iterator()
: const_iterator() {}

Run::iterator::~iterator() {}

Run::iterator::iterator(const Run::iterator& other)
: const_iterator(other) {}

Run::iterator::iterator(Run::iterator&& other)
: const_iterator(std::move(other)) {}

Run::iterator& Run::iterator::operator=(const Run::iterator& other) {
    if(this == &other) return *this;
    if(other.m_impl) 
        m_impl = std::make_unique<Impl>(*other.m_impl);
    else
        m_impl.reset();
    return *this;
}

Run::iterator& Run::iterator::operator=(Run::iterator&& other) {
    if(this == &other) return *this;
    m_impl = std::move(other.m_impl);
    return *this;
}

Run::iterator::reference Run::iterator::operator*() {
    return const_cast<reference>(const_iterator::operator*());
}

Run::iterator::pointer Run::iterator::operator->() {
    return const_cast<pointer>(const_iterator::operator->());
}

}
