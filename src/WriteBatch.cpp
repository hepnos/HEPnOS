/*
 * (C) 2019 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "hepnos.hpp"
#include "WriteBatchImpl.hpp"
#include "hepnos/AsyncEngine.hpp"

namespace hepnos {

WriteBatch::WriteBatch() {}

WriteBatch::WriteBatch(WriteBatch&& other)
: m_impl(std::move(other.m_impl)) {}

WriteBatch& WriteBatch::operator=(WriteBatch&& other) {
    if(&other == this) return *this;
    m_impl = std::move(other.m_impl);
    return *this;
}

WriteBatch::WriteBatch(DataStore& datastore, unsigned max_batch_size)
: m_impl(std::make_unique<WriteBatchImpl>(datastore.m_impl, max_batch_size)) {}

WriteBatch::WriteBatch(AsyncEngine& async, unsigned max_batch_size)
: m_impl(std::make_unique<WriteBatchImpl>(async.m_impl->m_datastore, max_batch_size, async.m_impl)) {}

WriteBatch::~WriteBatch() {}

void WriteBatch::flush() {
    if(m_impl)
        m_impl->flush();
}

void WriteBatch::activateStatistics(bool activate) {
    if(!m_impl)
        throw Exception("Invalid WriteBatch");
    if(activate) {
        if(m_impl->m_stats) return;
        m_impl->m_stats = std::make_unique<WriteBatchStatistics>();
    } else {
        m_impl->m_stats.reset();
    }
}

void WriteBatch::collectStatistics(WriteBatchStatistics& stats) const {
    if(m_impl)
        m_impl->collectStatistics(stats);
}

ProductID WriteBatch::storeRawData(const ProductID& key, const char* value, size_t vsize) {
    return m_impl->storeRawProduct(key, value, vsize);
}

bool WriteBatch::loadRawData(const ProductID& key, std::string& buffer) const {
    return m_impl->m_datastore->loadRawProduct(key, buffer);
}

bool WriteBatch::loadRawData(const ProductID& key, char* value, size_t* vsize) const {
    return m_impl->m_datastore->loadRawProduct(key, value, vsize);
}

bool WriteBatch::valid() const {
    return static_cast<bool>(m_impl);
}

}

std::ostream& operator<<(std::ostream& os, const hepnos::WriteBatchStatistics& stats)
{
    return os << "{ \"batch_size\" : " << stats.batch_sizes
              << ", \"key_sizes\" : " << stats.key_sizes
              << ", \"value_size\" : " << stats.value_sizes
              << " }";
}
