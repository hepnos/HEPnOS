#ifndef __HEPNOS_ASYNC_PREFETCHER_IMPL_HPP
#define __HEPNOS_ASYNC_PREFETCHER_IMPL_HPP

#include <set>
#include <unordered_set>
#include <unordered_map>
#include <thallium.hpp>
#include "DataStoreImpl.hpp"
#include "AsyncEngineImpl.hpp"

namespace tl = thallium;

namespace hepnos {

class AsyncPrefetcherImpl : public PrefetcherImpl {

    public:

    std::shared_ptr<AsyncEngineImpl>        m_async_engine;
    mutable tl::mutex                       m_item_cache_mtx;
    mutable tl::condition_variable          m_item_cache_cv;
    mutable bool                            m_item_prefetcher_active = false;

    mutable std::unordered_set<std::string> m_products_loading;
    mutable tl::mutex                       m_product_cache_mtx;
    mutable tl::condition_variable          m_product_cache_cv;

    AsyncPrefetcherImpl(const std::shared_ptr<DataStoreImpl>& ds,
                        const std::shared_ptr<AsyncEngineImpl>& async)
    : PrefetcherImpl(ds)
    , m_async_engine(async) {}

    /**
     * This ULT will fetch the requested product, then notify anyone waiting on
     * m_product_cache_cv that a new product is available.
     */
    void _product_prefetcher_thread(tl::pool& p, const ProductID& product_id) const {
        // load the product
        std::string data;
        bool ok = m_datastore->loadRawProduct(product_id, data);
        // update the cache
        {
            std::unique_lock<tl::mutex> lock(m_product_cache_mtx);
            if(ok) {
                m_product_cache[product_id.m_key] = std::move(data);
            }
            m_products_loading.erase(product_id.m_key);
        }
        // notify other ULTs of this update
        m_product_cache_cv.notify_one();
    }

    /**
     * This function spawns the _product_prefetcher_thread and an anonymous ULT.
     */
    void _spawn_product_prefetcher_threads(tl::pool& pool,
                                           const std::shared_ptr<ItemImpl>& item) const {
        auto& descriptor = item->m_descriptor;
        for(auto& key : m_active_product_keys) {
            auto product_id = DataStoreImpl::buildProductID(descriptor, key);
            {
                std::unique_lock<tl::mutex> lock(m_product_cache_mtx);
                auto it = m_products_loading.find(product_id.m_key);
                if(it != m_products_loading.end())
                    continue;
                // indicate that product is being loaded
                m_products_loading.insert(product_id.m_key);
            }
            // spawn a thread to load the product, but don't wait for it to complete
            pool.make_thread([pid=std::move(product_id), &pool, this]() {
                _product_prefetcher_thread(pool, pid);
                }, tl::anonymous());
        }
    }

    /**
     * This ULT prefetches items aftter a given one. It does so continuously
     * until told to shut down or until there isn't anymore items to fetch,
     * filling more of the item cache when space becomes available.
     */
    void _item_prefetcher_thread(tl::pool& p,
                                 const ItemType& item_type,
                                 const ItemType& prefix_type,
                                 const std::shared_ptr<ItemImpl>& after,
                                 int target) const {
        auto last = after;
        while(m_item_prefetcher_active) {
            // wait for space to be available in the cache
            {
                std::unique_lock<tl::mutex> lock(m_item_cache_mtx);
                while(m_item_cache.size() == m_cache_size) {
                    m_item_cache_cv.wait(lock);
                }
            }
            // we have space in the cache, fetch a batch of items
            std::vector<std::shared_ptr<ItemImpl>> items;
            size_t s = m_datastore->nextItems(item_type, prefix_type, last, items, m_batch_size, target);
            if(s != 0)
                last = items[items.size()-1];
            for(auto& item : items) {
                // start prefetching products (asynchronously)
                if(!m_active_product_keys.empty()) 
                    _spawn_product_prefetcher_threads(p, item);
                // lock the item cache and insert the item into it
                std::unique_lock<tl::mutex> lock(m_item_cache_mtx);
                m_item_cache.insert(std::move(item));
            }
            // notify anyone waiting that new items are available
            if(s < m_batch_size) {
                // no more items to get, no need to continue running this thread
                m_item_prefetcher_active = false;
            }
            m_item_cache_cv.notify_one();
        }
    }

    /**
     * This function spawns an anonymous ULT to continuously prefetch items
     * and associated products.
     */
    void _spawn_item_prefetcher_thread(tl::pool& p,
                                       const ItemType& item_type,
                                       const ItemType& prefix_type,
                                       const std::shared_ptr<ItemImpl>& current,
                                       int target) const {
        m_item_prefetcher_active = true;
        p.make_thread([&p, item_type, prefix_type, current, target, this]() {
                _item_prefetcher_thread(p, item_type, prefix_type, current, target);
                }, tl::anonymous());
    }

    /**
     * Initiates fetching products associated with an item.
     */
    void fetchRequestedProducts(const std::shared_ptr<ItemImpl>& itemImpl) const override {
        tl::pool& pool = m_async_engine->m_pool;
        _spawn_product_prefetcher_threads(pool, itemImpl);
    }

    /**
     * Initiate fetching items after a given one.
     */
    void prefetchFrom(const ItemType& item_type,
            const ItemType& prefix_type,
            const std::shared_ptr<ItemImpl>& current,
            int target=-1) const override
    {
        tl::pool& pool = m_async_engine->m_pool;
        _spawn_item_prefetcher_thread(pool, item_type, prefix_type, current, target);
    }

    /**
     * Get next items. prefetchFrom must have been called to initiate prefetching,
     * since this function waits for items to be made available by the prefetcher thread.
     */
    size_t nextItems(
            const ItemType& item_type,
            const ItemType& prefix_type,
            const std::shared_ptr<ItemImpl>& current,
            std::vector<std::shared_ptr<ItemImpl>>& result,
            size_t maxItems,
            int target=-1) const override
    {
        result.clear();
        auto last = current;
        while(result.size() < maxItems) {
            std::unique_lock<tl::mutex> lock(m_item_cache_mtx);
            auto ub = m_item_cache.upper_bound(last);
            if(ub == m_item_cache.end() && m_item_prefetcher_active) {
                // item not in cache but pefetcher is active
                m_item_cache_cv.wait(lock, [&ub, this, &last](){ 
                    ub = m_item_cache.upper_bound(last);
                    return (ub != m_item_cache.end()) || (!m_item_prefetcher_active);
                });
            }
            if(ub == m_item_cache.end()) {
                break;
            }
            // here we know we have found the next item
            auto it = ub;
            for(size_t i=0; i < maxItems && it != m_item_cache.end(); i++, it++) {
                result.push_back(*it);
            }
            last = result[result.size()-1];
            m_item_cache.erase(ub, it);
        }
        m_item_cache_cv.notify_one();
        return result.size();
    }

    virtual bool loadRawProduct(const ItemDescriptor& id,
                        const std::string& productName,
                        std::string& data) const override {
        auto product_id = DataStoreImpl::buildProductID(id, productName);
        std::unique_lock<tl::mutex> lock(m_product_cache_mtx);
        auto it = m_product_cache.find(product_id.m_key);
        if(it != m_product_cache.end()) {
            // product found right away
            auto& product = it->second;
            data = std::move(product);
            m_product_cache.erase(it);
        } else {
            // product not found, check if prefetching is pending
            if(m_products_loading.count(product_id.m_key) == 0) {
                // product is not currently being prefetched
                // try loading it from underlying DataStore
                return m_datastore->loadRawProduct(product_id, data);
            } else {
                // product is currently being prefetched,
                // wait for prefetching to complete
                m_product_cache_cv.wait(lock, [this, &product_id]() {
                           return m_products_loading.count(product_id.m_key) == 0; });
                // check again if the product is available
                it = m_product_cache.find(product_id.m_key);
                if(it == m_product_cache.end()) {
                    // product is not available
                    return false;
                }
                // product is available
                auto& product = it->second;
                data = std::move(product);
                m_product_cache.erase(it);
            }
        }
        return true;
    }

    virtual bool loadRawProduct(const ItemDescriptor& id,
                        const std::string& productName,
                        char* value, size_t* vsize) const override {
        auto product_id = DataStoreImpl::buildProductID(id, productName);
        std::unique_lock<tl::mutex> lock(m_product_cache_mtx);
        auto it = m_product_cache.find(product_id.m_key);
        if(it != m_product_cache.end()) {
            // product found right away
            auto& product = it->second;
            *vsize = product.size();
            std::memcpy(value, product.data(), *vsize);
            m_product_cache.erase(it);
        } else {
            // product not found, check if prefetching is pending
            if(m_products_loading.count(product_id.m_key) == 0) {
                // product is not currently being prefetched
                // try loading it from underlying DataStore
                return m_datastore->loadRawProduct(product_id, value, vsize);
            } else {
                // product is currently being prefetched,
                // wait for prefetching to complete
                m_product_cache_cv.wait(lock, [this, &product_id]() {
                           return m_products_loading.count(product_id.m_key) == 0; });
                // check again if the product is available
                it = m_product_cache.find(product_id.m_key);
                if(it == m_product_cache.end()) {
                    // product is not available
                    return false;
                }
                // product is available
                auto& product = it->second;
                *vsize = product.size();
                std::memcpy(value, product.data(), *vsize);
                m_product_cache.erase(it);
            }
        }
        return true;
    }
};

}

#endif
