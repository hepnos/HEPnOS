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

    mutable std::unique_ptr<PrefetcherStatistics> m_stats;
    mutable tl::mutex                             m_stats_mtx;

    std::shared_ptr<DataStoreImpl>   m_datastore;
    unsigned int                     m_cache_size = 16;
    unsigned int                     m_batch_size = 1;
    bool                             m_associated = false;
    std::vector<std::string>         m_active_product_keys;
    mutable std::set<std::shared_ptr<ItemImpl>, ItemPtrComparator> m_item_cache;
    mutable std::unordered_map<std::string, std::string> m_product_cache;

    PrefetcherImpl(const std::shared_ptr<DataStoreImpl>& ds)
    : m_datastore(ds) {}

    virtual ~PrefetcherImpl() = default;

    void update_batch_statistics(size_t batch_size) const {
        if(!m_stats) return;
        m_stats->batch_sizes.updateWith(batch_size);
    }

    void update_product_statistics(size_t psize) const {
        if(!m_stats) return;
        m_stats->product_sizes.updateWith(psize);
    }

    virtual void fetchRequestedProducts(const std::shared_ptr<ItemImpl>& itemImpl) const = 0;

    virtual void prefetchFrom(const ItemType& item_type,
            const ItemType& prefix_type,
            const std::shared_ptr<ItemImpl>& current,
            int target=-1) const = 0;

    virtual size_t nextItems(
            const ItemType& item_type,
            const ItemType& prefix_type,
            const std::shared_ptr<ItemImpl>& current,
            std::vector<std::shared_ptr<ItemImpl>>& result,
            size_t maxItems,
            int target=-1) const = 0;

    virtual bool loadRawProduct(const ItemDescriptor& id,
                        const std::string& productName,
                        std::string& data) const = 0;

    virtual bool loadRawProduct(const ItemDescriptor& id,
                        const std::string& productName,
                        char* value, size_t* vsize) const = 0;

    void collectStatistics(PrefetcherStatistics& stats) const {
        std::unique_lock<tl::mutex> lock(m_stats_mtx);
        if(m_stats)
            stats = *m_stats;
    }
};

}

#endif
