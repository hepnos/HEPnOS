#ifndef __HEPNOS_PREFETCHER_IMPL_HPP
#define __HEPNOS_PREFETCHER_IMPL_HPP

#include <set>
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
    mutable std::set<std::shared_ptr<ItemImpl>, ItemPtrComparator> m_cache;
    bool                             m_associated = false;

    PrefetcherImpl(const std::shared_ptr<DataStoreImpl>& ds)
    : m_datastore(ds) {}

    PrefetcherImpl(const std::shared_ptr<DataStoreImpl>& ds,
                   const std::shared_ptr<AsyncEngineImpl>& async)
    : m_datastore(ds)
    , m_async(async) {}

    void prefetchFrom(const ItemType& item_type,
            const ItemType& prefix_type,
            const std::shared_ptr<ItemImpl>& current,
            int target=-1) const
    {
        auto last = current;
        while(m_cache.size() != m_cache_size) {
            std::vector<std::shared_ptr<ItemImpl>> items;
            size_t s = m_datastore->nextItems(item_type, prefix_type, last, items, m_batch_size, target);
            if(s != 0)
                last = items[items.size()-1];
            for(auto& item : items) {
                m_cache.insert(std::move(item));
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
        auto ub = m_cache.upper_bound(current);
        if(ub == m_cache.end()) {
            m_cache.clear();
            prefetchFrom(item_type, prefix_type, current, target);
        }
        ub = m_cache.upper_bound(current);
        result.clear();
        if(ub == m_cache.end()) {
            return 0;
        } else {
            auto it = ub;
            result.clear();
            for(size_t i=0; i < maxItems && it != m_cache.end(); i++, it++) {
                result.push_back(*it);
            }
            m_cache.erase(ub, it);
        }
        return result.size();
    }
};

}

#endif
