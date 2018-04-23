#include <iomanip>
#include <sstream>
#include <string>
#include "hepnos/DataSet.hpp"
#include "hepnos/RunSet.hpp"
#include "private/DataSetImpl.hpp"
#include "private/DataStoreImpl.hpp"
#include "private/RunImpl.hpp"
#include "private/RunSetImpl.hpp"

namespace hepnos {

RunSet::RunSet(DataSet* ds)
: m_impl(std::make_unique<RunSet::Impl>(ds)) {}

RunSet::~RunSet() {}

RunSet::iterator RunSet::find(const RunNumber& runNumber) {
    int ret;
    std::vector<char> data;
    std::string parent = m_impl->m_dataset->fullname();
    std::string strNum = Run::Impl::makeKeyStringFromRunNumber(runNumber);
    auto datastore = m_impl->m_dataset->m_impl->m_datastore;
    auto level = m_impl->m_dataset->m_impl->m_level;
    bool b = datastore->m_impl->load(level+1, parent, strNum, data);
    if(!b) return end();
    return iterator(Run(datastore, level+1, parent, runNumber));
}

RunSet::const_iterator RunSet::find(const RunNumber& runNumber) const {
    iterator it = const_cast<RunSet*>(this)->find(runNumber);
    return it;
}

RunSet::iterator RunSet::begin() {
    auto it = find(0);
    if(it != end()) return *it;

    auto ds_level = m_impl->m_dataset->m_impl->m_level;
    auto datastore = m_impl->m_dataset->m_impl->m_datastore;
    std::string container = m_impl->m_dataset->fullname();
    Run run(datastore, ds_level+1, container, 0);
    run = run.next();

    if(run.valid()) return iterator(run);
    else return end();
}

RunSet::iterator RunSet::end() {
    return m_impl->m_end;
}

RunSet::const_iterator RunSet::cbegin() const {
    return const_iterator(const_cast<RunSet*>(this)->begin());
}

RunSet::const_iterator RunSet::cend() const {
    return m_impl->m_end;
}

RunSet::const_iterator RunSet::begin() const {
    return const_iterator(const_cast<RunSet*>(this)->begin());
}

RunSet::const_iterator RunSet::end() const {
    return m_impl->m_end;
}

RunSet::iterator RunSet::lower_bound(const RunNumber& lb) {
    if(lb == 0) {
        auto it = find(0);
        if(it != end()) {
            return it;
        } else {
            Run run(m_impl->m_dataset->m_impl->m_datastore, 
                    m_impl->m_dataset->m_impl->m_level+1,
                    m_impl->m_dataset->fullname(), 0);
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
        Run run(m_impl->m_dataset->m_impl->m_datastore,
                m_impl->m_dataset->m_impl->m_level+1,
                m_impl->m_dataset->fullname(), lb-1);
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
    Run run(m_impl->m_dataset->m_impl->m_datastore, 
            m_impl->m_dataset->m_impl->m_level+1, 
            m_impl->m_dataset->fullname(), ub);
    run = run.next();
    if(!run.valid()) return end();
    else return iterator(run);
}

RunSet::const_iterator RunSet::upper_bound(const RunNumber& ub) const {
    iterator it = const_cast<RunSet*>(this)->upper_bound(ub);
    return it;
}

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
