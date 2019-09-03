/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "hepnos/Run.hpp"
#include "private/RunImpl.hpp"
#include "private/SubRunImpl.hpp"
#include "private/DataStoreImpl.hpp"
#include "private/WriteBatchImpl.hpp"

namespace hepnos {

Run::Run()
: m_impl(std::make_unique<Run::Impl>(nullptr, 0, "", InvalidRunNumber)) {} 

Run::Run(DataStore* ds, uint8_t level, const std::string& container, const RunNumber& rn)
: m_impl(std::make_unique<Run::Impl>(ds, level, container, rn)) { }

Run::Run(const Run& other) {
    if(other.m_impl)
        m_impl = std::make_unique<Run::Impl>(*other.m_impl);
}

Run::Run(Run&&) = default;

Run& Run::operator=(const Run& other) {
    if(this == &other) return *this;
    if(other.m_impl) {
        m_impl = std::make_unique<Run::Impl>(*other.m_impl);
    }
    return *this;
}

Run& Run::operator=(Run&&) = default;

Run::~Run() = default; 

DataStore* Run::getDataStore() const {
    if(!valid()) { 
        throw Exception("Calling Run member function on an invalid Run object");
    }
    return m_impl->m_datastore;
}

Run Run::next() const {
    if(!valid()) return Run();
   
    std::vector<std::string> keys;
    size_t s = m_impl->m_datastore->m_impl->nextKeys(
            m_impl->m_level, m_impl->m_container, 
            m_impl->makeKeyStringFromRunNumber(), keys, 1);
    if(s == 0) return Run();
    size_t i = m_impl->m_container.size()+1;
    if(keys[0].size() <= i) return Run();
    if(keys[0][i] != '%') return Run();
    std::stringstream strRunNumber;
    strRunNumber << std::hex << keys[0].substr(i+1);
    RunNumber rn;
    strRunNumber >> rn;

    return Run(m_impl->m_datastore, m_impl->m_level, m_impl->m_container, rn);
}

bool Run::valid() const {
    return m_impl && m_impl->m_datastore; 

}

ProductID Run::storeRawData(const std::string& key, const std::string& buffer) {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    // forward the call to the datastore's store function
    return m_impl->m_datastore->m_impl->store(0, m_impl->fullpath(), key, buffer);
}

ProductID Run::storeRawData(std::string&& key, std::string&& buffer) {
    return storeRawData(key, buffer); // calls the above function
}

ProductID Run::storeRawData(WriteBatch& batch, const std::string& key, const std::string& buffer) {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    // forward the call to the datastore's store function
    return batch.m_impl->store(0, m_impl->fullpath(), key, buffer);
}

ProductID Run::storeRawData(WriteBatch& batch, std::string&& key, std::string&& buffer) {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    // forward the call to the datastore's store function
    return batch.m_impl->store(0, m_impl->fullpath(), std::move(key), std::move(buffer));
}

bool Run::loadRawData(const std::string& key, std::string& buffer) const {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    // forward the call to the datastore's load function
    return m_impl->m_datastore->m_impl->load(0, m_impl->fullpath(), key, buffer);
}

bool Run::operator==(const Run& other) const {
    bool v1 = valid();
    bool v2 = other.valid();
    if(!v1 && !v2) return true;
    if(v1 && !v2)  return false;
    if(!v1 && v2)  return false;
    return m_impl->m_datastore == other.m_impl->m_datastore
        && m_impl->m_level     == other.m_impl->m_level
        && m_impl->m_container == other.m_impl->m_container
        && m_impl->m_run_nr    == other.m_impl->m_run_nr;
}

bool Run::operator!=(const Run& other) const {
    return !(*this == other);
}

const RunNumber& Run::number() const {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    return m_impl->m_run_nr;
}

const std::string& Run::container() const {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    return m_impl->m_container;
}

SubRun Run::createSubRun(const SubRunNumber& subRunNumber) {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    std::string parent = m_impl->fullpath();
    std::string subRunStr = SubRun::Impl::makeKeyStringFromSubRunNumber(subRunNumber);
    m_impl->m_datastore->m_impl->store(m_impl->m_level+1, parent, subRunStr, std::string());
    return SubRun(m_impl->m_datastore, m_impl->m_level+1, parent, subRunNumber);
}

SubRun Run::createSubRun(WriteBatch& batch, const SubRunNumber& subRunNumber) {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    std::string parent = m_impl->fullpath();
    std::string subRunStr = SubRun::Impl::makeKeyStringFromSubRunNumber(subRunNumber);
    batch.m_impl->store(m_impl->m_level+1, parent, subRunStr, std::string());
    return SubRun(m_impl->m_datastore, m_impl->m_level+1, parent, subRunNumber);
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
    int ret;
    std::string parent = m_impl->fullpath();
    std::string subRunStr = SubRun::Impl::makeKeyStringFromSubRunNumber(subRunNumber);
    bool b = m_impl->m_datastore->m_impl->exists(m_impl->m_level+1, parent, subRunStr);
    if(!b) {
        return m_impl->m_end;
    }
    return iterator(SubRun(m_impl->m_datastore, m_impl->m_level+1, parent, subRunNumber));
}

Run::const_iterator Run::find(const SubRunNumber& subRunNumber) const {
    iterator it = const_cast<Run*>(this)->find(subRunNumber);
    return it;
}

Run::iterator Run::begin() {
    auto it = find(0);
    if(it != end()) return *it;

    auto level = m_impl->m_level;
    auto datastore = m_impl->m_datastore;
    std::string container = m_impl->fullpath();
    SubRun subrun(datastore, level+1, container, 0);
    subrun = subrun.next();

    if(subrun.valid()) return iterator(subrun);
    else return end();
}

Run::iterator Run::end() {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    return m_impl->m_end;
}

Run::const_iterator Run::begin() const {
    return const_iterator(const_cast<Run*>(this)->begin());
}

Run::const_iterator Run::end() const {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    return m_impl->m_end;
}

Run::const_iterator Run::cbegin() const {
    return const_iterator(const_cast<Run*>(this)->begin());
}

Run::const_iterator Run::cend() const {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    return m_impl->m_end;
}

Run::iterator Run::lower_bound(const SubRunNumber& lb) {
    if(lb == 0) {
        auto it = find(0);
        if(it != end()) {
            return it;
        } else {
            SubRun subrun(m_impl->m_datastore, 
                    m_impl->m_level+1,
                    m_impl->fullpath(), 0);
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
        SubRun subrun(m_impl->m_datastore,
                m_impl->m_level+1,
                m_impl->fullpath(), lb-1);
        subrun = subrun.next();
        if(!subrun.valid()) return end();
        else return iterator(subrun);
    }
}

Run::const_iterator Run::lower_bound(const SubRunNumber& lb) const {
    iterator it = const_cast<Run*>(this)->lower_bound(lb);
    return it;
}

Run::iterator Run::upper_bound(const SubRunNumber& ub) {
    if(!valid()) {
        throw Exception("Calling Run member function on an invalid Run object");
    }
    SubRun subrun(m_impl->m_datastore, 
            m_impl->m_level+1, 
            m_impl->fullpath(), ub);
    subrun = subrun.next();
    if(!subrun.valid()) return end();
    else return iterator(subrun);
}

Run::const_iterator Run::upper_bound(const SubRunNumber& ub) const {
    iterator it = const_cast<Run*>(this)->upper_bound(ub);
    return it;
}

////////////////////////////////////////////////////////////////////////////////////////////
// Run::const_iterator::Impl implementation
////////////////////////////////////////////////////////////////////////////////////////////

class Run::const_iterator::Impl {
    public:
        SubRun m_current_subrun;

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

        bool operator==(const Impl& other) const {
            return m_current_subrun == other.m_current_subrun;
        }
};

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
