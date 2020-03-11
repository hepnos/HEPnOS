#ifndef __HEPNOS_PREFETCHER_HPP
#define __HEPNOS_PREFETCHER_HPP

#include <memory>
#include <hepnos/Demangle.hpp>
#include <hepnos/Prefetchable.hpp>

namespace hepnos {

class DataStore;
class PrefetcherImpl;
class AsyncEngine;
class RunSet;
class EventSet;
class Run;
class SubRun;
class Event;

class Prefetcher {

    friend class RunSet;
    friend class EventSet;
    friend class Run;
    friend class SubRun;
    friend class Event;

    public:

    Prefetcher(const DataStore& ds,
               unsigned int cache_size=16,
               unsigned int batch_size=16);
    Prefetcher(const DataStore& ds, const AsyncEngine& async, 
               unsigned int cache_size=16,
               unsigned int batch_size=16);
    ~Prefetcher();
    Prefetcher(const Prefetcher&) = delete;
    Prefetcher(Prefetcher&&) = delete;
    Prefetcher& operator=(const Prefetcher&) = delete;
    Prefetcher& operator=(Prefetcher&&) = delete;

    unsigned int getCacheSize() const;
    void setCacheSize(unsigned int size);
    unsigned int getBatchSize() const;
    void setBatchSize(unsigned int size);

    template<typename Container>
    Prefetchable<Container> operator()(const Container& c) const {
        return Prefetchable<Container>(c, *this);
    }

    template<typename V>
    void fetchProduct(const std::string& label, bool fetch=true) const {
        fetchProductImpl(label + "#" + demangle<V>(), fetch);
    }

    private:

    std::shared_ptr<PrefetcherImpl> m_impl;

    void fetchProductImpl(const std::string& labelAndType, bool fetch) const;

};

}

#endif
