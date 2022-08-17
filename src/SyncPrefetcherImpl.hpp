/*
 * (C) 2019 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_SYNC_PREFETCHER_IMPL_HPP
#define __HEPNOS_SYNC_PREFETCHER_IMPL_HPP

#include <set>
#include <unordered_set>
#include <unordered_map>
#include "ProductCacheImpl.hpp"
#include "DataStoreImpl.hpp"
#include "AsyncEngineImpl.hpp"
#include "PrefetcherImpl.hpp"

namespace hepnos {

class SyncPrefetcherImpl : public PrefetcherImpl {

    public:

    SyncPrefetcherImpl(const std::shared_ptr<DataStoreImpl>& ds)
    : PrefetcherImpl(ds) {}

    void fetchRequestedProducts(const std::shared_ptr<ItemImpl>& itemImpl) const override {
        auto& descriptor = itemImpl->m_descriptor;
        for(auto& key : m_active_product_keys) {
            auto product_id = DataStoreImpl::makeProductID(
                descriptor, key.label.c_str(), key.label.size(),
                key.type.c_str(), key.type.size());
            if(m_product_cache.m_impl->hasProduct(product_id))
                continue;
            std::string data;
            bool ok = m_datastore->loadRawProduct(product_id, data);
            if(ok) {
                update_product_statistics(data.size());
                m_product_cache.m_impl->addRawProduct(product_id, std::move(data));
            }
        }
    }

    void prefetchFrom(const ItemType& item_type,
            const ItemType& prefix_type,
            const std::shared_ptr<ItemImpl>& current,
            int target=-1) const override
    {
        auto last = current;
        while(m_item_cache.size() != m_cache_size) {
            std::vector<std::shared_ptr<ItemImpl>> items;
            size_t s = m_datastore->nextItems(item_type, prefix_type, last, items, m_batch_size, target);
            if(s != 0) {
                update_batch_statistics(s);
                last = items[items.size()-1];
            }
            for(auto& item : items) {
                fetchRequestedProducts(item);
                m_item_cache.insert(std::move(item));
            }
            if(s < m_batch_size) break;
        }
    }

    size_t nextItems(
            const ItemType& item_type,
            const ItemType& prefix_type,
            const std::shared_ptr<ItemImpl>& current,
            std::vector<std::shared_ptr<ItemImpl>>& result,
            size_t maxItems,
            int target=-1) const override
    {
        auto ub = m_item_cache.upper_bound(current);
        if(ub == m_item_cache.end()) {
            m_item_cache.clear();
            prefetchFrom(item_type, prefix_type, current, target);
        }
        ub = m_item_cache.upper_bound(current);
        result.clear();
        if(ub == m_item_cache.end()) {
            return 0;
        } else {
            auto it = ub;
            result.clear();
            for(size_t i=0; i < maxItems && it != m_item_cache.end(); i++, it++) {
                result.push_back(*it);
            }
            m_item_cache.erase(ub, it);
        }
        return result.size();
    }

    bool loadRawProduct(const ProductID& product_id,
                        std::string& data) const override {
        if(m_product_cache.m_impl->loadRawProduct(product_id, data)) {
            if(m_stats) m_stats->product_cache_hit += 1;
            m_product_cache.m_impl->removeRawProduct(product_id);
            return true;
        } else {
            if(m_stats) m_stats->product_cache_miss += 1;
            return m_datastore->loadRawProduct(product_id, data);
        }
    }

    bool loadRawProduct(const ProductID& product_id,
                        char* value, size_t* vsize) const override {
        std::string data;
        if(m_product_cache.m_impl->loadRawProduct(product_id, data)) {
            if(m_stats) m_stats->product_cache_hit += 1;
            *vsize = data.size();
            if(*vsize) std::memcpy(value, data.data(), *vsize);
            m_product_cache.m_impl->removeRawProduct(product_id);
            return true;
        } else {
            if(m_stats) m_stats->product_cache_miss += 1;
            return m_datastore->loadRawProduct(product_id, value, vsize);
        }
    }
};

}

#endif
