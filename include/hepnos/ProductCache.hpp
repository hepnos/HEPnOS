/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRODUCT_CACHE_HPP
#define __HEPNOS_PRODUCT_CACHE_HPP

#include <memory>

namespace hepnos {

class Event;
class SubRun;
class Run;
class DataSet;
struct ProductCacheImpl;
struct ParallelEventProcessorImpl;
struct SyncPrefetcherImpl;
struct AsyncPrefetcherImpl;

/**
 * @brief The ProductCache is used in ParallelEventProcessor to
 * cache products associated with events.
 */
class ProductCache {

    friend class DataSet;
    friend class Run;
    friend class SubRun;
    friend class Event;
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
