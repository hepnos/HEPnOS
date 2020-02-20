/*
 * (C) 2019 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_ASYNC_ENGINE_H
#define __HEPNOS_ASYNC_ENGINE_H

#include <memory>

namespace hepnos {

class DataStore;
class DataSet;
class Run;
class SubRun;
class Event;
class WriteBatch;
class WriteBatchImpl;
class AsyncEngineImpl;

class AsyncEngine {

    friend class DataStore;
    friend class DataSet;
    friend class Run;
    friend class SubRun;
    friend class Event;
    friend class KeyValueContainer;
    friend class WriteBatch;

    private:

    std::shared_ptr<AsyncEngineImpl> m_impl;

    public:

    AsyncEngine(DataStore& ds, size_t num_threads=0);
    ~AsyncEngine() = default;
    AsyncEngine(const AsyncEngine&) = default;
    AsyncEngine& operator=(const AsyncEngine&) = default;
    AsyncEngine(AsyncEngine&&) = default;
    AsyncEngine& operator=(AsyncEngine&&) = default;

    void wait();
};

}

#endif
