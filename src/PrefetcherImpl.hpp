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
    unsigned int                     m_cache_size = 16;
    unsigned int                     m_batch_size = 1;
    bool                             m_associated = false;
    std::vector<std::string>         m_active_product_keys;
    mutable std::set<std::shared_ptr<ItemImpl>, ItemPtrComparator> m_item_cache;
    mutable std::unordered_map<std::string, std::string> m_product_cache;

    PrefetcherImpl(const std::shared_ptr<DataStoreImpl>& ds)
    : m_datastore(ds) {}

    virtual ~PrefetcherImpl() = default;

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
};

}

#endif
