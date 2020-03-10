#ifndef __HEPNOS_PREFETCHER_HPP
#define __HEPNOS_PREFETCHER_HPP

#include <memory>

namespace hepnos {

class DataStore;
class PrefetcherImpl;
class AsyncEngine;
class RunSet;
class EventSet;
class Run;
class SubRun;

class Prefetcher {

    friend class RunSet;
    friend class EventSet;
    friend class Run;
    friend class SubRun;

    private:

    std::shared_ptr<PrefetcherImpl> m_impl;

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
};

}

#endif
