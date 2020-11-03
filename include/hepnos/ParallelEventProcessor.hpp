/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PARALLEL_EVENT_PROCESSOR_HPP
#define __HEPNOS_PARALLEL_EVENT_PROCESSOR_HPP

#include <limits>
#include <mpi.h>
#include <hepnos/Demangle.hpp>
#include <hepnos/AsyncEngine.hpp>
#include <hepnos/Statistics.hpp>
#include <hepnos/DataStore.hpp>
#include <hepnos/ProductCache.hpp>

namespace hepnos {

struct ParallelEventProcessorImpl;

struct ParallelEventProcessorOptions {
    unsigned cacheSize       = std::numeric_limits<unsigned>::max(); // cache size of internal prefetcher
    unsigned inputBatchSize  = 16;                                   // size of batches loaded from HEPnOS
    unsigned outputBatchSize = 16;                                   // size of batches sent over MPI to workers
};

struct ParallelEventProcessorStatistics {
    size_t                    total_events_processed = 0;
    size_t                    local_events_processed = 0;
    double                    total_time             = 0.0; // total time in the process function, in seconds
    double                    total_processing_time  = 0.0; // total processing time, in seconds
    Statistics<double,double> processing_time_stats; // statistics on single-event processing times
    Statistics<double,double> waiting_time_stats; // statictics on time between calls to user-provided function
};

/**
 * @brief The ParallelEventProcessor enables running a function in parallel
 * on all the Events in a given container. The ParallelEventProcessor will
 * attempt to optimize accesses to the HEPnOS service and dispatch work to
 * members of the provided communicator to aim for load balancing.
 */
class ParallelEventProcessor {

    std::shared_ptr<ParallelEventProcessorImpl> m_impl;

    public:

    typedef std::function<void(const Event&)> EventProcessingFn;
    typedef std::function<void(const Event&, const ProductCache& cache)> EventProcessingWithCacheFn;

    /**
     * @brief Constructor. Builds a ParallelEventProcessor to navigate a dataset.
     * This constructor involves collective communications across members of the
     * provided communicator.
     *
     * @param datastore Datastore
     * @param comm Communicator gathering participating processes
     * @param options Options on how to carry out batching and dispatch
     */
    ParallelEventProcessor(const DataStore& datastore,
                           MPI_Comm comm,
                           const ParallelEventProcessorOptions& options = ParallelEventProcessorOptions());

    /**
     * @brief Constructor. Builds a ParallelEventProcessor to navigate a dataset.
     * This constructor involves collective communications across members of the
     * provided communicator.
     *
     * @param async AsyncEngine to use to access the storage in the background
     * @param comm Communicator gathering participating processes
     * @param options Options on how to carry out batching and dispatch
     */
    ParallelEventProcessor(const AsyncEngine& async,
                           MPI_Comm comm,
                           const ParallelEventProcessorOptions& options = ParallelEventProcessorOptions());

    /**
     * @brief Destructor. This destructor involves collective comunications
     * across the members of the underlying communicator.
     */
    ~ParallelEventProcessor();

    /**
     * @brief Tells the ParallelEventProcessor to preload objects of
     * certain type and with a certain label before executing the user callback.
     *
     * @tparam T Type of product to load
     * @param label Label associated with the product
     */
    template<typename T>
    void preload(const std::string& label) {
        std::string productKey = label + "#" + demangle<T>();
        preloadImpl(productKey);
    }

    /**
     * @brief Process all the events in the dataset. This function involves
     * collective communications across members of the underlying communicator.
     *
     * @param dataset Dataset in which to process events
     * @param function Function to execute on events
     * @param stats Pointer to a statistics object to fill
     */
    void process(const DataSet& dataset,
                 const EventProcessingWithCacheFn& function,
                 ParallelEventProcessorStatistics* stats = nullptr);

    void process(const DataSet& dataset,
                 const EventProcessingFn& function,
                 ParallelEventProcessorStatistics* stats = nullptr) {
        process(dataset,
                [&function](const Event& ev, const ProductCache&) {
                    function(ev);
                },
                stats);
    }

private:

    void preloadImpl(const std::string& productKey);
};

}

#endif
