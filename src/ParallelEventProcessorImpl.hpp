/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PARALLEL_EVENT_PROCESSOR_IMPL_HPP
#define __HEPNOS_PARALLEL_EVENT_PROCESSOR_IMPL_HPP

#include <queue>
#include <thallium.hpp>
#include "PrefetcherImpl.hpp"
#include "ProductCacheImpl.hpp"
#include "hepnos/EventSet.hpp"
#include "hepnos/ParallelEventProcessor.hpp"

namespace hepnos {

namespace tl = thallium;

struct ParallelEventProcessorImpl {

    std::shared_ptr<DataStoreImpl>  m_datastore;
    MPI_Comm                        m_comm;
    ParallelEventProcessorOptions   m_options;
    std::vector<int>                m_loader_ranks;
    std::vector<int>                m_targets;
    std::unordered_set<std::string> m_product_keys;

    bool                            m_loader_running = false;
    std::queue<EventDescriptor>     m_event_queue;
    tl::mutex                       m_event_queue_mtx;
    tl::condition_variable          m_event_queue_cv;

    int                             m_num_active_consumers;
    tl::managed<tl::xstream>        m_mpi_xstream;

    ParallelEventProcessorStatistics* m_stats = nullptr;

    ParallelEventProcessorImpl(
            std::shared_ptr<DataStoreImpl> ds,
            MPI_Comm comm,
            const ParallelEventProcessorOptions& options)
    : m_datastore(std::move(ds))
    , m_comm(comm)
    , m_options(options) {
        MPI_Comm_size(comm, &m_num_active_consumers);
        m_num_active_consumers -= 1;
    }

    ~ParallelEventProcessorImpl() {}

    /**
     * Main function to start processing events in parallel.
     */
    void process(const std::vector<EventSet>& evsets,
                 const ParallelEventProcessor::EventProcessingWithCacheFn& function,
                 ParallelEventProcessorStatistics* stats) {
        m_stats = stats;
        startLoadingEventsFromTargets(evsets);
        startRespondingToMPIrequests();
        processEvents(function);
        if(!m_mpi_xstream->is_null()) {
            m_mpi_xstream->join();
        }
        m_stats = nullptr;
    }

    /**
     * Starts the ULT that loads events from HEPnOS. This ULT is posted
     * on the first pool of the current ES.
     */
    void startLoadingEventsFromTargets(const std::vector<EventSet>& evsets) {
        if(evsets.size() == 0) {
            return;
        }
        m_loader_running = true;
        tl::xstream::self().make_thread([this, evsets](){
            loadEventsFromTargets(evsets);
        }, tl::anonymous());
    }

    /**
     * Content of the ULT that loads events from HEPnOS. This ULT
     * will loop over the EventSets, and inside an EventSet over the
     * Events, and push descriptors inside the event queue.
     */
    void loadEventsFromTargets(const std::vector<EventSet>& evsets) {
        for(auto& evset : evsets) {

            Prefetcher prefetcher(
                    DataStore(m_datastore),
                    m_options.cacheSize,
                    m_options.inputBatchSize);

            for(auto it = evset.begin(prefetcher); it != evset.end(); it++) {
                EventDescriptor descriptor;
                it->toDescriptor(descriptor);
                {
                    std::lock_guard<tl::mutex> lock(m_event_queue_mtx);
                    m_event_queue.push(descriptor);
                }
                m_event_queue_cv.notify_all();
            }
        }
        {
            std::lock_guard<tl::mutex> lock(m_event_queue_mtx);
            m_loader_running = false;
            m_event_queue_cv.notify_all();
        }
    }

    /**
     * Starts the ES that responds to MPI requests from other clients.
     * This has to be done in a separate ES because MPI isn't Argobots-aware.
     */
    void startRespondingToMPIrequests() {
        if(!m_loader_running)
            return;
        m_mpi_xstream = tl::xstream::create();
        m_mpi_xstream->make_thread([this](){
            respondToMPIrequests();
        }, tl::anonymous());
    }

    /**
     * Function called in the above ES. This ULT will wait for requests from
     * clients that don't have local events to process anymore.
     */
    void respondToMPIrequests() {
        int my_rank;
        MPI_Comm_rank(m_comm, &my_rank);
        while(m_num_active_consumers != 0) {
            MPI_Status status;
            size_t num_events_requested;
            MPI_Recv(&num_events_requested, sizeof(num_events_requested),
                     MPI_BYTE, MPI_ANY_SOURCE, 1111, m_comm, &status);
            if(num_events_requested == 0) num_events_requested = 1;
            int consumer_rank = status.MPI_SOURCE;
            std::vector<EventDescriptor> descriptorsToSend;
            descriptorsToSend.reserve(num_events_requested);
            {
                std::unique_lock<tl::mutex> lock(m_event_queue_mtx);
                while(m_loader_running && m_event_queue.empty())
                     m_event_queue_cv.wait(lock);
                for(unsigned i = 0; i < num_events_requested && !m_event_queue.empty(); i++) {
                    descriptorsToSend.push_back(m_event_queue.front());
                    m_event_queue.pop();
                }
            }
            MPI_Send(descriptorsToSend.data(),
                     descriptorsToSend.size()*sizeof(descriptorsToSend[0]),
                     MPI_BYTE, consumer_rank, 1112, m_comm);
            if(descriptorsToSend.empty()) {
                m_num_active_consumers -= 1;
            }
        }
    }

    /**
     * This function tries to fill out the provided vector with a batch of
     * Event descriptors taken locally or from loader processes.
     * It returns true if new EventDescriptors were put in the vector,
     * false otherwise.
     */
    bool requestEvents(std::vector<EventDescriptor>& descriptors) {
        int my_rank;
        MPI_Comm_rank(m_comm, &my_rank);
        while(m_loader_ranks.size() != 0) {
            int loader_rank = m_loader_ranks[0];
            if(loader_rank == my_rank) {
                std::unique_lock<tl::mutex> lock(m_event_queue_mtx);
                while(m_event_queue.empty() && m_loader_running)
                     m_event_queue_cv.wait(lock);
                size_t num_events_requested = m_options.outputBatchSize;
                descriptors.resize(num_events_requested);
                size_t num_actual_events = 0;
                for(unsigned i = 0; i < num_events_requested && !m_event_queue.empty(); i++) {
                    descriptors[i] = m_event_queue.front();
                    m_event_queue.pop();
                    num_actual_events += 1;
                    if(m_stats) m_stats->local_events_processed += 1;
                }
                if(num_actual_events != 0) {
                    descriptors.resize(num_actual_events);
                    return true;
                } else {
                    m_loader_ranks.erase(m_loader_ranks.begin());
                }
            } else {
                size_t num_events_requested = m_options.outputBatchSize;
                MPI_Send(&num_events_requested, sizeof(num_events_requested), MPI_BYTE, loader_rank, 1111, m_comm);
                MPI_Status status;
                descriptors.resize(num_events_requested);
                MPI_Recv(descriptors.data(), sizeof(descriptors[0])*descriptors.size(),
                         MPI_BYTE, loader_rank, 1112, m_comm, &status);
                int count;
                MPI_Get_count(&status, MPI_BYTE, &count);
                size_t num_actual_events = count/sizeof(descriptors[0]);
                if(num_actual_events != 0) {
                    descriptors.resize(num_actual_events);
                    return true;
                } else {
                    m_loader_ranks.erase(m_loader_ranks.begin());
                }
            }
        }
        return false;
    }

    void preloadProductsFor(const ItemDescriptor& descriptor, ProductCache& cache) {
        for(auto& product_key : m_product_keys) {
            auto product_id = DataStoreImpl::buildProductID(descriptor, product_key);
            std::string data;
            bool ok = m_datastore->loadRawProduct(product_id, data);
            if(ok) {
                cache.m_impl->addRawProduct(descriptor, product_key, std::move(data));
            }
        }
    }

    /**
     * This function keeps requesting new events and call the user-provided callback.
     */
    void processEvents(const ParallelEventProcessor::EventProcessingWithCacheFn& user_function) {
        if(m_stats) *m_stats = ParallelEventProcessorStatistics();
        double t_start = tl::timer::wtime();
        std::vector<EventDescriptor> descriptors;
        double t1;
        double t2 = tl::timer::wtime();
        ProductCache cache;
        while(requestEvents(descriptors)) {
            for(auto& d : descriptors) {
                cache.clear();
                Event event = Event::fromDescriptor(DataStore(m_datastore), d, false);
                preloadProductsFor(d, cache);
                t1 = tl::timer::wtime();
                if(m_stats) m_stats->waiting_time_stats.updateWith(t1-t2);
                user_function(event, cache);
                t2 = tl::timer::wtime();
                if(m_stats) {
                    m_stats->processing_time_stats.updateWith(t2 - t1);
                    m_stats->total_processing_time += t2 - t1;
                    m_stats->total_events_processed += 1;
                }
            }
        }
        double t_end = tl::timer::wtime();
        if(m_stats) m_stats->total_time = t_end - t_start;
    }
};

}

#endif
