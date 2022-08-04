/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PREFETCHER_HPP
#define __HEPNOS_PREFETCHER_HPP

#include <memory>
#include <hepnos/Demangle.hpp>
#include <hepnos/Prefetchable.hpp>
#include <hepnos/Statistics.hpp>
#include <hepnos/RawStorage.hpp>

namespace hepnos {

class DataStore;
class PrefetcherImpl;
class AsyncEngine;
class RunSet;
class EventSet;
class Run;
class SubRun;
class Event;

struct PrefetcherStatistics {
    Statistics<size_t,double> batch_sizes;
    Statistics<size_t,double> product_sizes;
    size_t                    product_cache_hit  = 0;
    size_t                    product_cache_miss = 0;
};

/**
 * @brief The Prefetcher object will actively try to prefetch
 * items from the underlying DataStore when using iterators.
 */
class Prefetcher : public RawStorage {

    friend class RunSet;
    friend class EventSet;
    friend class Run;
    friend class SubRun;
    friend class Event;

    public:

    /**
     * @brief Constructor.
     *
     * @param ds DataStore instance to prefetch from.
     * @param cache_size maximum number of items that can be stored in the cache.
     * @param batch_size how many items to prefetch at once.
     */
    Prefetcher(const DataStore& ds,
               unsigned int cache_size=16,
               unsigned int batch_size=16);

    /**
     * @brief Constructor using an AsyncEngine to prefetch in the background.
     *
     * @param async AsyncEngine to prefetch in the background.
     * @param cache_size maximum number of items that can be stored in the cache.
     * @param batch_size how many items to prefetch at once.
     */
    Prefetcher(const AsyncEngine& async,
               unsigned int cache_size=16,
               unsigned int batch_size=16);

    /**
     * @brief Destructor.
     */
    ~Prefetcher();

    /**
     * @brief Deleted copy constructor.
     */
    Prefetcher(const Prefetcher&) = default;

    /**
     * @brief Deleted move constructor.
     */
    Prefetcher(Prefetcher&&) = default;

    /**
     * @brief Deleted copy-assignment operator.
     */
    Prefetcher& operator=(const Prefetcher&) = delete;

    /**
     * @brief Deleted move-assignment operator.
     */
    Prefetcher& operator=(Prefetcher&&) = delete;

    /**
     * @return Cache size.
     */
    unsigned int getCacheSize() const;

    /**
     * @brief Set the cache size.
     *
     * @param size new size.
     */
    void setCacheSize(unsigned int size);

    /**
     * @return Batch size.
     */
    unsigned int getBatchSize() const;

    /**
     * @brief Set the batch size.
     *
     * @param size new size.
     */
    void setBatchSize(unsigned int size);

    /**
     * @brief Creates a Prefetachable container from a container.
     *
     * @tparam Container Container type (RunSet, Run, SubRun)
     * @param c container.
     *
     * @return a Prefetchable container.
     */
    template<typename Container>
    Prefetchable<Container> operator()(const Container& c) const {
        return Prefetchable<Container>(c, *this);
    }

    /**
     * @brief If fetch is true, instruct the Prefetcher to also prefetch
     * products of a given type with the given label along with the items.
     * If fetch is false, instruct not to prefetch products of the given
     * type and label.
     *
     * @tparam V Type of product.
     * @param label Label of the product.
     * @param fetch Whether to prefetch or not.
     */
    template<typename V>
    void fetchProduct(const std::string& label, bool fetch=true) const {
        fetchProductImpl(label, demangle<V>(), fetch);
    }

    /**
     * @brief Activate statistics collection.
     *
     * @param activate Whether to activate statistics.
     */
    void activateStatistics(bool activate=true);

    /**
     * @brief Collects the usage statistics.
     *
     * @param stats PrefetcherStatistics object to fill.
     */
    void collectStatistics(PrefetcherStatistics& stats) const;

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

    private:

    std::shared_ptr<PrefetcherImpl> m_impl;

    void fetchProductImpl(const std::string& label, const std::string& type, bool fetch) const;

};

}

namespace fmt {

template<>
struct formatter<hepnos::PrefetcherStatistics> {

    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin(), end = ctx.end();
        while(it != end && *it != '}') it++;
        return it;
    }

    template<typename FormatContext>
    auto format(const hepnos::PrefetcherStatistics& stats, FormatContext& ctx) {
        return format_to(ctx.out(), "{{ \"batch_sizes\" : {}, "
                                       "\"product_sizes\" : {}, "
                                       "\"product_cache_hit\" : {}, "
                                       "\"product_cache_miss\" : {} }}",
                                       stats.batch_sizes,
                                       stats.product_sizes,
                                       stats.product_cache_hit,
                                       stats.product_cache_miss);
    }

};

}

#endif
