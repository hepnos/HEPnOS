/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "hepnos/DataSet.hpp"
#include "hepnos/Run.hpp"
#include "hepnos/RunSet.hpp"
#include "hepnos/AsyncEngine.hpp"
#include "hepnos/Prefetcher.hpp"
#include "ItemImpl.hpp"
#include "DataSetImpl.hpp"
#include "EventSetImpl.hpp"
#include "DataStoreImpl.hpp"
#include "AsyncEngineImpl.hpp"
#include "WriteBatchImpl.hpp"
#include "PrefetcherImpl.hpp"

namespace hepnos {

DataSet::iterator DataSetImpl::m_end;

DataSet::DataSet()
: m_impl(std::make_shared<DataSetImpl>(nullptr, 0, std::make_shared<std::string>(""), "")) {}

DataSet::DataSet(const std::shared_ptr<DataSetImpl>& impl)
: m_impl(impl) {}

DataSet::DataSet(std::shared_ptr<DataSetImpl>&& impl)
: m_impl(std::move(impl)) {}

DataStore DataSet::datastore() const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    return DataStore(m_impl->m_datastore);
}

DataSet DataSet::next() const {
    if(!valid()) return DataSet();
    std::vector<std::shared_ptr<DataSetImpl>> result;
    size_t s = m_impl->m_datastore->nextDataSets(m_impl, result, 1);
    if(s == 0) return DataSet();
    else return DataSet(std::move(result[0]));
}

bool DataSet::valid() const {
    return m_impl && m_impl->m_datastore;
}

ProductID DataSet::storeRawData(const std::string& key, const char* value, size_t vsize) {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    // forward the call to the datastore's store function
    ItemDescriptor id(m_impl->m_uuid);
    return m_impl->m_datastore->storeRawProduct(id, key, value, vsize);
}

ProductID DataSet::storeRawData(WriteBatch& batch, const std::string& key, const char* value, size_t vsize) {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    // forward the call to the datastore's store function
    ItemDescriptor id(m_impl->m_uuid);
    if(batch.m_impl)
        return batch.m_impl->storeRawProduct(id, key, value, vsize);
    else
        return m_impl->m_datastore->storeRawProduct(id, key, value, vsize);
}

ProductID DataSet::storeRawData(AsyncEngine& async, const std::string& key, const char* value, size_t vsize) {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    // forward the call to the async engine's store function
    ItemDescriptor id(m_impl->m_uuid);
    if(async.m_impl)
        return async.m_impl->storeRawProduct(id, key, value, vsize);
    else
        return m_impl->m_datastore->storeRawProduct(id, key, value, vsize);
}

bool DataSet::loadRawData(const std::string& key, std::string& buffer) const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    // forward the call to the datastore's load function
    ItemDescriptor id(m_impl->m_uuid);
    return m_impl->m_datastore->loadRawProduct(id, key, buffer);
}

bool DataSet::loadRawData(const std::string& key, char* value, size_t* vsize) const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    // forward the call to the datastore's load function
    ItemDescriptor id(m_impl->m_uuid);
    return m_impl->m_datastore->loadRawProduct(id, key, value, vsize);
}

bool DataSet::loadRawData(const Prefetcher& prefetcher, const std::string& key, std::string& buffer) const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    (void)prefetcher; // prefetcher isn't usable with a DataSet
    ItemDescriptor id(m_impl->m_uuid);
    return m_impl->m_datastore->loadRawProduct(id, key, buffer);
}

bool DataSet::loadRawData(const Prefetcher& prefetcher, const std::string& key, char* value, size_t* vsize) const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    (void)prefetcher; // prefetcher isn't usable with a DataSet
    ItemDescriptor id(m_impl->m_uuid);
    return m_impl->m_datastore->loadRawProduct(id, key, value, vsize);
}

bool DataSet::operator==(const DataSet& other) const {
    bool v1 = valid();
    bool v2 = other.valid();
    if(!v1 && !v2) return true;
    if(v1  && !v2) return false;
    if(!v1 &&  v2) return false;
    return m_impl->m_datastore  == other.m_impl->m_datastore
        && m_impl->m_level      == other.m_impl->m_level
        && (m_impl->m_container == other.m_impl->m_container
            || *m_impl->m_container == *other.m_impl->m_container )
        && m_impl->m_name       == other.m_impl->m_name;
}

bool DataSet::operator!=(const DataSet& other) const {
    return !(*this == other);
}

const std::string& DataSet::name() const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    return m_impl->m_name;
}

const std::string& DataSet::container() const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    return *m_impl->m_container;
}

std::string DataSet::fullname() const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    return m_impl->fullname();
}

DataSet DataSet::createDataSet(const std::string& name) {
    if(name.find('/') != std::string::npos
    || name.find('%') != std::string::npos) {
        throw Exception("Invalid character '/' or '%' in dataset name");
    }
    std::string parent = fullname();
    auto new_dataset_impl = std::make_shared<DataSetImpl>(
            m_impl->m_datastore, m_impl->m_level+1,
            std::make_shared<std::string>(parent), name);
    bool b = m_impl->m_datastore->createDataSet(m_impl->m_level+1, parent, name, new_dataset_impl->m_uuid);
    if(!b) {
        return *find(name);
    }
    return DataSet(new_dataset_impl);
}

Run DataSet::createRun(const RunNumber& runNumber) {
    if(InvalidRunNumber == runNumber) {
        throw Exception("Trying to create a Run with InvalidRunNumber");
    }
    m_impl->m_datastore->createItem(m_impl->m_uuid, runNumber);
    return Run(std::make_shared<ItemImpl>(
                    m_impl->m_datastore,
                    m_impl->m_uuid,
                    runNumber));
}

Run DataSet::createRun(WriteBatch& batch, const RunNumber& runNumber) {
    if(InvalidRunNumber == runNumber) {
        throw Exception("Trying to create a Run with InvalidRunNumber");
    }
    if(batch.m_impl)
        batch.m_impl->createItem(m_impl->m_uuid, runNumber);
    else
        m_impl->m_datastore->createItem(m_impl->m_uuid, runNumber);
    return Run(std::make_shared<ItemImpl>(
                    m_impl->m_datastore,
                    m_impl->m_uuid,
                    runNumber));
}

Run DataSet::createRun(AsyncEngine& async, const RunNumber& runNumber) {
    if(InvalidRunNumber == runNumber) {
        throw Exception("Trying to create a Run with InvalidRunNumber");
    }
    if(async.m_impl)
        async.m_impl->createItem(m_impl->m_uuid, runNumber);
    else
        m_impl->m_datastore->createItem(m_impl->m_uuid, runNumber);
    return Run(std::make_shared<ItemImpl>(
                    m_impl->m_datastore,
                    m_impl->m_uuid,
                    runNumber));
}

DataSet DataSet::operator[](const std::string& datasetName) const {
    auto it = find(datasetName);
    if(!it->valid())
        throw Exception("Requested DataSet does not exist");
    return std::move(*it);
}

Run DataSet::operator[](const RunNumber& runNumber) const {
    auto it = runs().find(runNumber);
    if(!it->valid())
        throw Exception("Requested Run does not exist");
    return std::move(*it);
}

DataSet::iterator DataSet::find(const std::string& datasetPath) {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    int ret;
    if(datasetPath.find('%') != std::string::npos) {
        throw Exception("Invalid character '%' in dataset name");
    }

    size_t slash_count = std::count(datasetPath.begin(), datasetPath.end(), '/');
    size_t level = m_impl->m_level + 1 + slash_count;
    std::string containerName;
    std::string datasetName;

    std::string parent = fullname();

    if(slash_count == 0) {
        datasetName = datasetPath;
        containerName = parent;
    } else {
        size_t c = datasetPath.find_last_of('/');
        containerName = parent + "/" + datasetPath.substr(0,c);
        datasetName   = datasetPath.substr(c+1);
    }

    UUID uuid;
    bool b = m_impl->m_datastore->loadDataSet(level, containerName, datasetName, uuid);
    if(!b) {
        return DataSetImpl::m_end;
    }
    return iterator(
                DataSet(
                    std::make_shared<DataSetImpl>(
                        m_impl->m_datastore,
                        level,
                        std::make_shared<std::string>(containerName),
                        datasetName,
                        uuid)));
}

DataSet::const_iterator DataSet::find(const std::string& datasetName) const {
    iterator it = const_cast<DataSet*>(this)->find(datasetName);
    return it;
}

DataSet::iterator DataSet::begin() {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    // we use the prefix "&" because we need something that comes after "%"
    // (which represents runs) and is not going to be in a dataset name
    DataSet ds(
            std::make_shared<DataSetImpl>(
                m_impl->m_datastore,
                m_impl->m_level+1,
                std::make_shared<std::string>(fullname()),
                "&"));

    ds = ds.next();
    if(ds.valid()) return iterator(ds);
    else return end();
}

DataSet::iterator DataSet::end() {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    return DataSetImpl::m_end;
}

DataSet::const_iterator DataSet::begin() const {
    return const_iterator(const_cast<DataSet*>(this)->begin());
}

DataSet::const_iterator DataSet::end() const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    return DataSetImpl::m_end;
}

DataSet::const_iterator DataSet::cbegin() const {
    return const_iterator(const_cast<DataSet*>(this)->begin());
}

DataSet::const_iterator DataSet::cend() const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    return DataSetImpl::m_end;
}

DataSet::iterator DataSet::lower_bound(const std::string& lb) {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    std::string lb2 = lb;
    size_t s = lb2.size();
    lb2[s-1] -= 1; // sdskv_list_keys's start_key is exclusive
    iterator it = find(lb2);
    if(it != end()) {
        // we found something before the specified lower bound
        ++it;
        return it;
    }
    DataSet ds(
            std::make_shared<DataSetImpl>(
                m_impl->m_datastore,
                m_impl->m_level+1,
                std::make_shared<std::string>(fullname()),
                lb2));

    ds = ds.next();
    if(!ds.valid()) return end();
    else return iterator(ds);
}

DataSet::const_iterator DataSet::lower_bound(const std::string& lb) const {
    iterator it = const_cast<DataSet*>(this)->lower_bound(lb);
    return it;
}

DataSet::iterator DataSet::upper_bound(const std::string& ub) {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    DataSet ds(
            std::make_shared<DataSetImpl>(
                m_impl->m_datastore,
                m_impl->m_level+1,
                std::make_shared<std::string>(fullname()), ub));

    ds = ds.next();
    if(!ds.valid()) return end();
    else return iterator(ds);
}

DataSet::const_iterator DataSet::upper_bound(const std::string& ub) const {
    iterator it = const_cast<DataSet*>(this)->upper_bound(ub);
    return it;
}

RunSet DataSet::runs() const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    return RunSet(m_impl);
}

EventSet DataSet::events(int target) const {
    if(!valid()) {
        throw Exception("Calling DataSet member function on an invalid DataSet");
    }
    auto numTargets = m_impl->m_datastore->numTargets(ItemType::EVENT);
    if(target >= (int)numTargets) {
        throw Exception(std::string("Invalid target number ")
                        +std::to_string(target)
                        +" for EventSet (>= "
                        +std::to_string(numTargets)+")");
    }
    if(target >= 0)
        return EventSet(std::make_shared<EventSetImpl>(*m_impl, target));
    else {
        return EventSet(std::make_shared<EventSetImpl>(*m_impl, 0, numTargets));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////
// DataSet::const_iterator::Impl implementation
////////////////////////////////////////////////////////////////////////////////////////////

class DataSet::const_iterator::Impl {
    public:
        DataSet    m_current_dataset;

        Impl()
        : m_current_dataset()
        {}

        Impl(const DataSet& dataset)
        : m_current_dataset(dataset)
        {}

        Impl(DataSet&& dataset)
        : m_current_dataset(std::move(dataset))
        {}

        Impl(const Impl& other)
        : m_current_dataset(other.m_current_dataset)
        {}

        bool operator==(const Impl& other) const {
            return m_current_dataset == other.m_current_dataset;
        }
};

DataSet::const_iterator::const_iterator()
: m_impl(std::make_unique<Impl>()) {}

DataSet::const_iterator::const_iterator(const DataSet& dataset)
: m_impl(std::make_unique<Impl>(dataset)) {}

DataSet::const_iterator::const_iterator(DataSet&& dataset)
: m_impl(std::make_unique<Impl>(std::move(dataset))) {}

DataSet::const_iterator::~const_iterator() {}

DataSet::const_iterator::const_iterator(const DataSet::const_iterator& other) {
    if(other.m_impl) {
        m_impl = std::make_unique<Impl>(*other.m_impl);
    }
}

DataSet::const_iterator::const_iterator(DataSet::const_iterator&& other)
: m_impl(std::move(other.m_impl)) {}

DataSet::const_iterator& DataSet::const_iterator::operator=(const DataSet::const_iterator& other) {
    if(&other == this) return *this;
    if(other.m_impl)
        m_impl = std::make_unique<Impl>(*other.m_impl);
    else
        m_impl.reset();
    return *this;
}

DataSet::const_iterator& DataSet::const_iterator::operator=(DataSet::const_iterator&& other) {
    if(&other == this) return *this;
    m_impl = std::move(other.m_impl);
    return *this;
}

DataSet::const_iterator::self_type DataSet::const_iterator::operator++() {
    if(!m_impl) {
        throw Exception("Trying to increment an invalid iterator");
    }
    m_impl->m_current_dataset = m_impl->m_current_dataset.next();
    return *this;
}

DataSet::const_iterator::self_type DataSet::const_iterator::operator++(int) {
    const_iterator copy = *this;
    ++(*this);
    return copy;
}

const DataSet::const_iterator::reference DataSet::const_iterator::operator*() {
    if(!m_impl) {
        throw Exception("Trying to dereference an invalid iterator");
    }
    return m_impl->m_current_dataset;
}
const DataSet::const_iterator::pointer DataSet::const_iterator::operator->() {
    if(!m_impl) {
        throw Exception("Trying to dereference an invalid iterator");
    }
    return &(m_impl->m_current_dataset);
}

bool DataSet::const_iterator::operator==(const self_type& rhs) const {
    if(!m_impl && !rhs.m_impl)  return true;
    if(m_impl  && !rhs.m_impl)  return false;
    if(!m_impl && rhs.m_impl)   return false;
    return *m_impl == *(rhs.m_impl);
}

bool DataSet::const_iterator::operator!=(const self_type& rhs) const {
    return !(*this == rhs);
}

////////////////////////////////////////////////////////////////////////////////////////////
// DataStore::iterator implementation
////////////////////////////////////////////////////////////////////////////////////////////

DataSet::iterator::iterator(const DataSet& current)
: const_iterator(current) {}

DataSet::iterator::iterator(DataSet&& current)
: const_iterator(std::move(current)) {}

DataSet::iterator::iterator()
: const_iterator() {}

DataSet::iterator::~iterator() {}

DataSet::iterator::iterator(const DataSet::iterator& other)
: const_iterator(other) {}

DataSet::iterator::iterator(DataSet::iterator&& other)
: const_iterator(std::move(other)) {}

DataSet::iterator& DataSet::iterator::operator=(const DataSet::iterator& other) {
    if(this == &other) return *this;
    if(other.m_impl)
        m_impl = std::make_unique<Impl>(*other.m_impl);
    else
        m_impl.reset();
    return *this;
}

DataSet::iterator& DataSet::iterator::operator=(DataSet::iterator&& other) {
    if(this == &other) return *this;
    m_impl = std::move(other.m_impl);
    return *this;
}

DataSet::iterator::reference DataSet::iterator::operator*() {
    return const_cast<reference>(const_iterator::operator*());
}

DataSet::iterator::pointer DataSet::iterator::operator->() {
    return const_cast<pointer>(const_iterator::operator->());
}

}
