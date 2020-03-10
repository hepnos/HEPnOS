/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include <iomanip>
#include <sstream>
#include <string>
#include "hepnos/DataSet.hpp"
#include "hepnos/RunSet.hpp"
#include "hepnos/Prefetcher.hpp"
#include "DataSetImpl.hpp"
#include "DataStoreImpl.hpp"
#include "ItemImpl.hpp"
#include "PrefetcherImpl.hpp"

namespace hepnos {

////////////////////////////////////////////////////////////////////////////////////////////
// RunSet::const_iterator::Impl declaration
////////////////////////////////////////////////////////////////////////////////////////////

class RunSet::const_iterator::Impl {
    public:
        Run m_current_run;
        std::shared_ptr<PrefetcherImpl> m_prefetcher;

        Impl()
        : m_current_run()
        {}

        Impl(const Run& run)
        : m_current_run(run)
        {}

        Impl(Run&& run)
            : m_current_run(std::move(run))
        {}

        Impl(const Impl& other)
            : m_current_run(other.m_current_run)
        {}

        ~Impl() {
            if(m_prefetcher)
                m_prefetcher->m_associated = false;
        }

        bool operator==(const Impl& other) const {
            return m_current_run == other.m_current_run;
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

////////////////////////////////////////////////////////////////////////////////////////////
// RunSet implementation
////////////////////////////////////////////////////////////////////////////////////////////

static RunSet::iterator RunSet_end;

RunSet::RunSet(const std::shared_ptr<RunSetImpl>& impl)
: m_impl(impl) {}

RunSet::RunSet(std::shared_ptr<RunSetImpl>&& impl)
: m_impl(std::move(impl)) {}

Run RunSet::operator[](const RunNumber& runNumber) {
    auto it = find(runNumber);
    if(!it->valid())
        throw Exception("Requested Run does not exist");
    return std::move(*it);
}

DataStore RunSet::datastore() const {
    return DataStore(m_impl->m_datastore);
}

RunSet::iterator RunSet::find(const RunNumber& runNumber) {
    int ret;
    auto& datastore = m_impl->m_datastore;
    bool b = datastore->itemExists(m_impl->m_uuid, runNumber);
    if(!b) return end();
    return iterator(
            std::make_shared<ItemImpl>(
                datastore,
                m_impl->m_uuid,
                runNumber));
}

RunSet::iterator RunSet::find(const RunNumber& runNumber, const Prefetcher& prefetcher) {
    auto it = find(runNumber);
    if(it != end())
        it.m_impl->setPrefetcher(prefetcher.m_impl);
    return it;
}

RunSet::const_iterator RunSet::find(const RunNumber& runNumber) const {
    iterator it = const_cast<RunSet*>(this)->find(runNumber);
    return it;
}

RunSet::const_iterator RunSet::find(const RunNumber& runNumber, const Prefetcher& prefetcher) const {
    iterator it = const_cast<RunSet*>(this)->find(runNumber);
    if(it != end())
        it.m_impl->setPrefetcher(prefetcher.m_impl);
    return it;
}

RunSet::iterator RunSet::begin() {
    auto it = find(0);
    if(it != end()) return it;

    auto ds_level = m_impl->m_level;
    auto datastore = m_impl->m_datastore;
    auto new_run_impl = std::make_shared<ItemImpl>(datastore, m_impl->m_uuid, 0);
    Run run(std::move(new_run_impl));
    run = run.next();

    if(run.valid()) return iterator(run);
    else return end();
}

RunSet::iterator RunSet::begin(const Prefetcher& prefetcher) {
    auto it = begin();
    if(it != end())
        it.m_impl->setPrefetcher(prefetcher.m_impl);
    return it;
}

RunSet::iterator RunSet::end() {
    return RunSet_end;
}

RunSet::const_iterator RunSet::cbegin() const {
    return const_iterator(const_cast<RunSet*>(this)->begin());
}

RunSet::const_iterator RunSet::cbegin(const Prefetcher& prefetcher) const {
    auto it = cbegin();
    if(it != cend())
        it.m_impl->setPrefetcher(prefetcher.m_impl);
    return it;
}

RunSet::const_iterator RunSet::cend() const {
    return RunSet_end;
}

RunSet::const_iterator RunSet::begin() const {
    return const_iterator(const_cast<RunSet*>(this)->begin());
}

RunSet::const_iterator RunSet::begin(const Prefetcher& prefetcher) const {
    auto it = const_iterator(const_cast<RunSet*>(this)->begin());
    if(it != end())
        it.m_impl->setPrefetcher(prefetcher.m_impl);
    return it;
}

RunSet::const_iterator RunSet::end() const {
    return RunSet_end;
}

RunSet::iterator RunSet::lower_bound(const RunNumber& lb) {
    if(lb == 0) {
        auto it = find(0);
        if(it != end()) {
            return it;
        } else {
            Run run(std::make_shared<ItemImpl>(
                    m_impl->m_datastore, 
                    m_impl->m_uuid, 0));
            run = run.next();
            if(!run.valid()) return end();
            else return iterator(run);
        }
    } else {
        auto it = find(lb-1);
        if(it != end()) {
            ++it;
            return it;
        }
        Run run(std::make_shared<ItemImpl>(
                m_impl->m_datastore, 
                m_impl->m_uuid, lb-1));
        run = run.next();
        if(!run.valid()) return end();
        else return iterator(run);
    }
}

RunSet::iterator RunSet::lower_bound(const RunNumber& lb, const Prefetcher& prefetcher) {
    auto it = lower_bound(lb);
    if(it != end())
        it.m_impl->setPrefetcher(prefetcher.m_impl);
    return it;
}

RunSet::const_iterator RunSet::lower_bound(const RunNumber& lb) const {
    iterator it = const_cast<RunSet*>(this)->lower_bound(lb);
    return it;
}

RunSet::const_iterator RunSet::lower_bound(const RunNumber& lb, const Prefetcher& prefetcher) const {
    iterator it = const_cast<RunSet*>(this)->lower_bound(lb);
    if(it != end())
        it.m_impl->setPrefetcher(prefetcher.m_impl);
    return it;
}

RunSet::iterator RunSet::upper_bound(const RunNumber& ub) {
    Run run(std::make_shared<ItemImpl>(m_impl->m_datastore, 
                                      m_impl->m_uuid, ub));
    run = run.next();
    if(!run.valid()) return end();
    else return iterator(run);
}

RunSet::iterator RunSet::upper_bound(const RunNumber& ub, const Prefetcher& prefetcher) {
    auto it = upper_bound(ub);
    if(it != end())
        it.m_impl->setPrefetcher(prefetcher.m_impl);
    return it;
}

RunSet::const_iterator RunSet::upper_bound(const RunNumber& ub) const {
    iterator it = const_cast<RunSet*>(this)->upper_bound(ub);
    return it;
}


RunSet::const_iterator RunSet::upper_bound(const RunNumber& ub, const Prefetcher& prefetcher) const {
    iterator it = const_cast<RunSet*>(this)->upper_bound(ub);
    if(it != end())
        it.m_impl->setPrefetcher(prefetcher.m_impl);
    return it;
}


////////////////////////////////////////////////////////////////////////////////////////////
// RunSet::const_iterator implementation
////////////////////////////////////////////////////////////////////////////////////////////

RunSet::const_iterator::const_iterator()
: m_impl(std::make_unique<Impl>()) {}

RunSet::const_iterator::const_iterator(const Run& run)
: m_impl(std::make_unique<Impl>(run)) {}

RunSet::const_iterator::const_iterator(Run&& run)
: m_impl(std::make_unique<Impl>(std::move(run))) {}

RunSet::const_iterator::~const_iterator() {}

RunSet::const_iterator::const_iterator(const RunSet::const_iterator& other)
: m_impl(std::make_unique<Impl>(*other.m_impl)) {}

RunSet::const_iterator::const_iterator(RunSet::const_iterator&& other)
: m_impl(std::move(other.m_impl)) {}

RunSet::const_iterator& RunSet::const_iterator::operator=(const RunSet::const_iterator& other) {
    if(&other == this) return *this;
    m_impl = std::make_unique<Impl>(*other.m_impl);
    return *this;
}

RunSet::const_iterator& RunSet::const_iterator::operator=(RunSet::const_iterator&& other) {
    if(&other == this) return *this;
    m_impl = std::move(other.m_impl);
    return *this;
}

RunSet::const_iterator::self_type RunSet::const_iterator::operator++() {
    if(!m_impl) {
        throw Exception("Trying to increment an invalid iterator");
    }
    if(!m_impl->m_prefetcher)
        m_impl->m_current_run = m_impl->m_current_run.next();
    else {
        std::vector<std::shared_ptr<ItemImpl>> next_runs;
        size_t s = m_impl->m_prefetcher->nextItems(ItemType::RUN, 
                ItemType::DATASET, m_impl->m_current_run.m_impl, next_runs, 1);
        if(s == 1) {
            m_impl->m_current_run.m_impl = std::move(next_runs[0]);
        } else {
            m_impl->m_current_run = Run();
        }
    }
    return *this;
}

RunSet::const_iterator::self_type RunSet::const_iterator::operator++(int) {
    const_iterator copy = *this;
    ++(*this);
    return copy;
}

const RunSet::const_iterator::reference RunSet::const_iterator::operator*() {
    if(!m_impl) {
        throw Exception("Trying to dereference an invalid iterator");
    }
    return m_impl->m_current_run;
}

const RunSet::const_iterator::pointer RunSet::const_iterator::operator->() {
    if(!m_impl) return nullptr;
    return &(m_impl->m_current_run);
}

bool RunSet::const_iterator::operator==(const self_type& rhs) const {
    if(!m_impl && !rhs.m_impl)  return true;
    if(m_impl  && !rhs.m_impl)  return false;
    if(!m_impl && rhs.m_impl)   return false;
    return *m_impl == *(rhs.m_impl);
}

bool RunSet::const_iterator::operator!=(const self_type& rhs) const {
        return !(*this == rhs);
}

////////////////////////////////////////////////////////////////////////////////////////////
// RunSet::iterator implementation
////////////////////////////////////////////////////////////////////////////////////////////

RunSet::iterator::iterator(const Run& current)
: const_iterator(current) {}

RunSet::iterator::iterator(Run&& current)
: const_iterator(std::move(current)) {}

RunSet::iterator::iterator()
: const_iterator() {}

RunSet::iterator::~iterator() {}

RunSet::iterator::iterator(const RunSet::iterator& other)
: const_iterator(other) {}

RunSet::iterator::iterator(RunSet::iterator&& other)
: const_iterator(std::move(other)) {}

RunSet::iterator& RunSet::iterator::operator=(const RunSet::iterator& other) {
    if(this == &other) return *this;
    m_impl = std::make_unique<Impl>(*other.m_impl);
    return *this;
}

RunSet::iterator& RunSet::iterator::operator=(RunSet::iterator&& other) {
    if(this == &other) return *this;
    m_impl = std::move(other.m_impl);
    return *this;
}

RunSet::iterator::reference RunSet::iterator::operator*() {
    return const_cast<reference>(const_iterator::operator*());
}

RunSet::iterator::pointer RunSet::iterator::operator->() {
    return const_cast<pointer>(const_iterator::operator->());
}

}
