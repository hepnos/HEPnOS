/*
 * (C) 2019 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "hepnos/Prefetcher.hpp"
#include "hepnos/AsyncEngine.hpp"
#include "SyncPrefetcherImpl.hpp"
#include "AsyncPrefetcherImpl.hpp"

namespace hepnos {

Prefetcher::Prefetcher(const DataStore& ds, unsigned int cache_size, unsigned int batch_size)
: m_impl(std::make_shared<SyncPrefetcherImpl>(ds.m_impl)) {
    m_impl->m_cache_size = cache_size;
    m_impl->m_batch_size = batch_size;
}

Prefetcher::Prefetcher(const AsyncEngine& async, unsigned int cache_size, unsigned int batch_size)
: m_impl(std::make_shared<AsyncPrefetcherImpl>(async.m_impl->m_datastore, async.m_impl)) {
    m_impl->m_cache_size = cache_size;
    m_impl->m_batch_size = batch_size;
}

Prefetcher::~Prefetcher() {}

unsigned int Prefetcher::getCacheSize() const {
    return m_impl->m_cache_size;
}

void Prefetcher::setCacheSize(unsigned int size) {
    m_impl->m_cache_size = size;
}

unsigned int Prefetcher::getBatchSize() const {
    return m_impl->m_batch_size;
}

void Prefetcher::setBatchSize(unsigned int size) {
    m_impl->m_batch_size = size;
}

void Prefetcher::fetchProductImpl(const std::string& label, const std::string& type, bool fetch=true) const {
    auto& v = m_impl->m_active_product_keys;
    auto product_key = ProductKey{label, type};
    auto it = std::find(v.begin(), v.end(), product_key);
    if(fetch) {
        if(it != v.end())
            v.push_back(product_key);
    } else {
        if(it != v.end())
            v.erase(it);
    }
}

void Prefetcher::activateStatistics(bool activate) {
    if(activate) {
        if(m_impl->m_stats) return;
        m_impl->m_stats = std::make_unique<PrefetcherStatistics>();
    } else {
        m_impl->m_stats.reset();
    }
}

void Prefetcher::collectStatistics(PrefetcherStatistics& stats) const {
    m_impl->collectStatistics(stats);
}

ProductID Prefetcher::storeRawData(const ProductID& key, const char* value, size_t vsize) {
    return m_impl->m_datastore->storeRawProduct(key, value, vsize);
}

bool Prefetcher::loadRawData(const ProductID& key, std::string& buffer) const {
    return m_impl->loadRawProduct(key, buffer);
}

bool Prefetcher::loadRawData(const ProductID& key, char* value, size_t* vsize) const {
    return m_impl->loadRawProduct(key, value, vsize);
}

bool Prefetcher::valid() const {
    return static_cast<bool>(m_impl);
}

}
