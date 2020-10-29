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

void Prefetcher::fetchProductImpl(const std::string& label, bool fetch=true) const {
    auto& v = m_impl->m_active_product_keys;
    auto it = std::find(v.begin(), v.end(), label);
    if(fetch) {
        if(it != v.end())
            v.push_back(label);
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

}
