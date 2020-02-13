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
#include "DataSetImpl.hpp"
#include "DataStoreImpl.hpp"
#include "RunImpl.hpp"

namespace hepnos {

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
    std::string parent = m_impl->fullname();
    std::string strNum = makeKeyStringFromNumber(runNumber);
    auto datastore = m_impl->m_datastore;
    auto level = m_impl->m_level;
    bool b = datastore->exists(level+1, parent, strNum);
    if(!b) return end();
    return iterator(
            std::make_shared<RunImpl>(
                datastore,
                level+1,
                std::make_shared<std::string>(parent),
                runNumber));
}

RunSet::const_iterator RunSet::find(const RunNumber& runNumber) const {
    iterator it = const_cast<RunSet*>(this)->find(runNumber);
    return it;
}

RunSet::iterator RunSet::begin() {
    auto it = find(0);
    if(it != end()) return *it;

    auto ds_level = m_impl->m_level;
    auto datastore = m_impl->m_datastore;
    std::string container = m_impl->fullname();
    auto new_run_impl = std::make_shared<RunImpl>(datastore,
            ds_level+1, std::make_shared<std::string>(container), 0);
    Run run(std::move(new_run_impl));
    run = run.next();

    if(run.valid()) return iterator(run);
    else return end();
}

RunSet::iterator RunSet::end() {
    return RunSet_end;
}

RunSet::const_iterator RunSet::cbegin() const {
    return const_iterator(const_cast<RunSet*>(this)->begin());
}

RunSet::const_iterator RunSet::cend() const {
    return RunSet_end;
}

RunSet::const_iterator RunSet::begin() const {
    return const_iterator(const_cast<RunSet*>(this)->begin());
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
            Run run(std::make_shared<RunImpl>(
                    m_impl->m_datastore, 
                    m_impl->m_level+1,
                    std::make_shared<std::string>(m_impl->fullname()), 0));
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
        Run run(std::make_shared<RunImpl>(
                m_impl->m_datastore, 
                m_impl->m_level+1,
                std::make_shared<std::string>(m_impl->fullname()), lb-1));
        run = run.next();
        if(!run.valid()) return end();
        else return iterator(run);
    }
}

RunSet::const_iterator RunSet::lower_bound(const RunNumber& lb) const {
    iterator it = const_cast<RunSet*>(this)->lower_bound(lb);
    return it;
}

RunSet::iterator RunSet::upper_bound(const RunNumber& ub) {
    Run run(std::make_shared<RunImpl>(m_impl->m_datastore, 
            m_impl->m_level+1, 
            std::make_shared<std::string>(m_impl->fullname()), ub));
    run = run.next();
    if(!run.valid()) return end();
    else return iterator(run);
}

RunSet::const_iterator RunSet::upper_bound(const RunNumber& ub) const {
    iterator it = const_cast<RunSet*>(this)->upper_bound(ub);
    return it;
}

////////////////////////////////////////////////////////////////////////////////////////////
// RunSet::const_iterator::Impl implementation
////////////////////////////////////////////////////////////////////////////////////////////

class RunSet::const_iterator::Impl {
    public:
        Run m_current_run;

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

        bool operator==(const Impl& other) const {
            return m_current_run == other.m_current_run;
        }
};

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
    m_impl->m_current_run = m_impl->m_current_run.next();
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
