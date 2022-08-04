/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "hepnos/ProductCache.hpp"
#include "DataStoreImpl.hpp"
#include "ProductCacheImpl.hpp"
#include <thallium.hpp>

namespace hepnos {

namespace tl = thallium;

ProductCache::ProductCache(const DataStore& ds)
: m_impl(std::make_shared<ProductCacheImpl>(ds.m_impl)) {}

ProductCache::~ProductCache() = default;

ProductCache::ProductCache(const ProductCache&) = default;

ProductCache::ProductCache(ProductCache&&) = default;

ProductCache& ProductCache::operator=(const ProductCache&) = default;

ProductCache& ProductCache::operator=(ProductCache&&) = default;

void ProductCache::clear() {
    auto& impl = *m_impl;
    impl.m_lock.wrlock();
    impl.m_map.clear();
    impl.m_lock.unlock();
}

size_t ProductCache::size() const {
    auto& impl = *m_impl;
    impl.m_lock.rdlock();
    size_t s = impl.m_map.size();
    impl.m_lock.unlock();
    return s;
}

ProductID ProductCache::storeRawData(const ProductID& key, const char* value, size_t vsize) {
    return m_impl->m_datastore->storeRawProduct(key, value, vsize);
}

bool ProductCache::loadRawData(const ProductID& key, std::string& buffer) const {
    return m_impl->loadRawProduct(key, buffer);
}

bool ProductCache::loadRawData(const ProductID& key, char* value, size_t* vsize) const {
    return m_impl->loadRawProduct(key, value, vsize);
}

bool ProductCache::valid() const {
    return static_cast<bool>(m_impl);
}

}
