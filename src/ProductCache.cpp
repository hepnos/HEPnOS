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

ProductCache::ProductCache()
: m_impl(std::make_shared<ProductCacheImpl>()) {}

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

}
