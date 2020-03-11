#ifndef __HEPNOS_PREFETCHER_IMPL_HPP
#define __HEPNOS_PREFETCHER_IMPL_HPP

#include <set>
#include <unordered_set>
#include <unordered_map>
#include "DataStoreImpl.hpp"
#include "AsyncEngineImpl.hpp"

namespace hepnos {

class PrefetcherImpl {

    public:

    struct ItemPtrComparator {
        bool operator()(const std::shared_ptr<ItemImpl>& lhs,
                      const std::shared_ptr<ItemImpl>& rhs) const {
            return lhs->m_descriptor < rhs->m_descriptor;
        }
    };

    std::shared_ptr<DataStoreImpl>   m_datastore;
    std::shared_ptr<AsyncEngineImpl> m_async;
    unsigned int                     m_cache_size = 16;
    unsigned int                     m_batch_size = 1;
    bool                             m_associated = false;
    std::unordered_set<std::string>  m_active_product_keys;
    mutable std::set<std::shared_ptr<ItemImpl>, ItemPtrComparator> m_item_cache;
    mutable std::unordered_map<std::string, std::string> m_product_cache;

    PrefetcherImpl(const std::shared_ptr<DataStoreImpl>& ds)
    : m_datastore(ds) {}

    PrefetcherImpl(const std::shared_ptr<DataStoreImpl>& ds,
                   const std::shared_ptr<AsyncEngineImpl>& async)
    : m_datastore(ds)
    , m_async(async) {}

    void fetchRequestedProducts(const std::shared_ptr<ItemImpl>& itemImpl) const {
        auto& descriptor = itemImpl->m_descriptor;
        for(auto& key : m_active_product_keys) {
            auto product_id = DataStoreImpl::buildProductID(descriptor, key);
            auto it = m_product_cache.find(product_id.m_key);
            if(it != m_product_cache.end())
                continue;
            std::string data;
            bool ok = m_datastore->loadRawProduct(product_id, data);
            if(ok) {
                m_product_cache[product_id.m_key] = std::move(data);
            }
        }
    }

    void prefetchFrom(const ItemType& item_type,
            const ItemType& prefix_type,
            const std::shared_ptr<ItemImpl>& current,
            int target=-1) const
    {
        auto last = current;
        while(m_item_cache.size() != m_cache_size) {
            std::vector<std::shared_ptr<ItemImpl>> items;
            size_t s = m_datastore->nextItems(item_type, prefix_type, last, items, m_batch_size, target);
            if(s != 0)
                last = items[items.size()-1];
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
            int target=-1) const
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

    bool loadRawProduct(const ItemDescriptor& id,
                        const std::string& productName,
                        std::string& data) const {
        auto product_id = DataStoreImpl::buildProductID(id, productName);
        auto it = m_product_cache.find(product_id.m_key);
        if(it == m_product_cache.end()) {
            return m_datastore->loadRawProduct(product_id, data);
        } else {
            data = std::move(it->second);
            m_product_cache.erase(it);
            return true;
        }
    }

    bool loadRawProduct(const ItemDescriptor& id,
                        const std::string& productName,
                        char* value, size_t* vsize) const {
        auto product_id = DataStoreImpl::buildProductID(id, productName);
        auto it = m_product_cache.find(product_id.m_key);
        if(it == m_product_cache.end()) {
            return m_datastore->loadRawProduct(id, productName, value, vsize);
        } else {
            *vsize = it->second.size();
            std::memcpy(value, it->second.data(), *vsize);
            return true;
        }
    }
};

}

#endif
