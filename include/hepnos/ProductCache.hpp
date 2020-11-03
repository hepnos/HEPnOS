/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRODUCT_CACHE_HPP
#define __HEPNOS_PRODUCT_CACHE_HPP

#include <memory>

namespace hepnos {

struct ProductCacheImpl;
struct ParallelEventProcessorImpl;
struct SyncPrefetcherImpl;
struct AsyncPrefetcherImpl;

class ProductCache {

    friend struct ParallelEventProcessorImpl;
    friend struct SyncPrefetcherImpl;
    friend struct AsyncPrefetcherImpl;

    std::shared_ptr<ProductCacheImpl> m_impl;

    public:

    ProductCache();

    ~ProductCache();

    ProductCache(const ProductCache&);

    ProductCache(ProductCache&&);

    ProductCache& operator=(const ProductCache&);

    ProductCache& operator=(ProductCache&&);

    void clear();

    size_t size() const;
};

}

#endif
