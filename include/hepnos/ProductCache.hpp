/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRODUCT_CACHE_HPP
#define __HEPNOS_PRODUCT_CACHE_HPP

#include <memory>
#include <hepnos/RawStorage.hpp>

namespace hepnos {

class DataStore;
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
class ProductCache : public RawStorage {

    friend class DataSet;
    friend class Run;
    friend class SubRun;
    friend class Event;
    friend struct ParallelEventProcessorImpl;
    friend struct SyncPrefetcherImpl;
    friend struct AsyncPrefetcherImpl;

    std::shared_ptr<ProductCacheImpl> m_impl;

    public:

    ProductCache(const DataStore& ds);

    ~ProductCache();

    ProductCache(const ProductCache&);

    ProductCache(ProductCache&&);

    ProductCache& operator=(const ProductCache&);

    ProductCache& operator=(ProductCache&&);

    void clear();

    size_t size() const;

    bool valid() const override;

    protected:
    /**
     * @see RawStorage::storeRawData
     */
    ProductID storeRawData(const ProductID& key, const char* value, size_t vsize) override;

    /**
     * @see RawStorage::loadRawData
     */
    bool loadRawData(const ProductID& key, std::string& buffer) const override;

    /**
     * @see RawStorage::loadRawData
     */
    bool loadRawData(const ProductID& key, char* value, size_t* vsize) const override;
};

}

#endif
