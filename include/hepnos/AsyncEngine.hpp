/*
 * (C) 2019 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_ASYNC_ENGINE_H
#define __HEPNOS_ASYNC_ENGINE_H

#include <memory>
#include <vector>

namespace hepnos {

class DataStore;
class DataSet;
class Run;
class SubRun;
class Event;
class WriteBatch;
class WriteBatchImpl;
class Prefetcher;
class PrefetcherImpl;
class AsyncEngineImpl;

/**
 * @brief The AsyncEngine class uses Argobots to provide a set
 * of execution streams (ES) to perform operations in the background.
 * This object may then be used by Prefetcher or WriteBatch
 * instances to perform their RPCs in the background.
 * The AsyncEngine may be initialized with 0 ES, in which case
 * the communications will still happen in the background using
 * Mercury's non-blocking functions, but the rest of the processing
 * will happen synchronously.
 */
class AsyncEngine {

    friend class DataStore;
    friend class DataSet;
    friend class Run;
    friend class SubRun;
    friend class Event;
    friend class KeyValueContainer;
    friend class WriteBatch;
    friend class Prefetcher;

    private:

    std::shared_ptr<AsyncEngineImpl> m_impl;

    public:

    /**
     * @brief Constructor.
     *
     * @param ds DataStore instance.
     * @param num_threads Number of execution streams (background threads) to start.
     */
    AsyncEngine(DataStore& ds, size_t num_threads=0);

    /**
     * @brief Destructor.
     */
    ~AsyncEngine() = default;

    /**
     * @brief Copy constructor. A copy of an existing
     * AsyncEngine will share the same pool of threads as the
     * instance it was copied from.
     *
     * @param AsyncEngine AsyncEngine instance to copy.
     */
    AsyncEngine(const AsyncEngine&) = default;

    /**
     * @brief Copy-assignment operator. A copy of
     * an existing AsyncEngine will share the same pool of
     * threads as the instance it was copied from.
     *
     * @param AsyncEngine AsyncEngine to copy.
     *
     * @return The assigned-to instance.
     */
    AsyncEngine& operator=(const AsyncEngine&) = default;

    /**
     * @brief Move constructor. The thread pool will be
     * moved to the new AsyncEngine instance, leaving the
     * moved-from instance in an invalid state.
     *
     * @param AsyncEngine AsyncEngine to move from.
     */
    AsyncEngine(AsyncEngine&&) = default;

    /**
     * @brief Move-assignment operator. The thread pool
     * will be moved to the new moved-to AsyncEngine
     * instance, leaving the moved-from instance in an
     * invalid state.
     *
     * @param AsyncEngine AsyncEngine to move from.
     *
     * @return The moved-to instance.
     */
    AsyncEngine& operator=(AsyncEngine&&) = default;

    /**
     * @brief Blocks until the threads in this AsyncEngine
     * don't have any more work to do.
     */
    void wait();

    /**
     * @brief Returns the list of errors that occured
     * when running the asynchronous work.
     *
     * @return List of error messages.
     */
    const std::vector<std::string>& errors() const;
};

}

#endif
