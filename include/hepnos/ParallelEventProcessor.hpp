/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PARALLEL_EVENT_PROCESSOR_HPP
#define __HEPNOS_PARALLEL_EVENT_PROCESSOR_HPP

#include <mpi.h>
#include <hepnos/Prefetcher.hpp>
#include <hepnos/DataStore.hpp>

namespace hepnos {

struct ParallelEventProcessorImpl;

struct DispatchPolicy {
    size_t eventsPerBlock = 16;
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

    /**
     * @brief Constructor. Builds a ParallelEventProcessor to navigate a dataset.
     * This constructor involves collective communications across members of the
     * provided communicator.
     *
     * @param datastore Datastore
     * @param comm Communicator gathering participating processes
     * @param prefetcher Prefetcher to use when reading events from storage
     * @param policy Dispatch policy to use when sending events to workers
     */
    ParallelEventProcessor(const DataStore& datastore,
                           MPI_Comm comm,
                           const Prefetcher& prefetcher,
                           const DispatchPolicy& policy = DispatchPolicy());

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
                 const EventProcessingFn& function,
                 ParallelEventProcessorStatistics* stats = nullptr);

private:

    void preloadImpl(const std::string& productKey);
};

}

#endif
