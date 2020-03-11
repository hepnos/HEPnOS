#include "hepnos/Prefetcher.hpp"
#include "hepnos/AsyncEngine.hpp"
#include "PrefetcherImpl.hpp"

namespace hepnos {

Prefetcher::Prefetcher(const DataStore& ds, unsigned int cache_size, unsigned int batch_size)
: m_impl(std::make_shared<PrefetcherImpl>(ds.m_impl)) {
    m_impl->m_cache_size = cache_size;
    m_impl->m_batch_size = batch_size;
}

Prefetcher::Prefetcher(const DataStore& ds, const AsyncEngine& async, unsigned int cache_size, unsigned int batch_size)
: m_impl(std::make_shared<PrefetcherImpl>(ds.m_impl, async.m_impl)) {
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
    if(fetch) {
        m_impl->m_active_product_keys.insert(label);
    } else {
        m_impl->m_active_product_keys.erase(label);
    }
}

}
