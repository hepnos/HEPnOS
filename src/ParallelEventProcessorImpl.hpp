/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PARALLEL_EVENT_PROCESSOR_IMPL_HPP
#define __HEPNOS_PARALLEL_EVENT_PROCESSOR_IMPL_HPP

#include <numeric>
#include <queue>
#include <thallium.hpp>
#include <spdlog/spdlog.h>
#include "ProductKey.hpp"
#include "PrefetcherImpl.hpp"
#include "ProductCacheImpl.hpp"
#include "hepnos/EventSet.hpp"
#include "hepnos/ParallelEventProcessor.hpp"
#include <thallium/serialization/stl/vector.hpp>

namespace hepnos {

template<typename Archive>
inline void save(Archive& ar, const EventDescriptor& desc) {
    ar.write(desc.data, hepnos::EventDescriptorLength);
}

template<typename Archive>
inline void load(Archive& ar, EventDescriptor& desc) {
    ar.read(desc.data, hepnos::EventDescriptorLength);
}


namespace tl = thallium;

struct ParallelEventProcessorImpl : public tl::provider<ParallelEventProcessorImpl> {

    std::shared_ptr<DataStoreImpl>    m_datastore;
    std::shared_ptr<AsyncEngineImpl>  m_async;
    MPI_Comm                          m_comm;
    int                               m_my_rank;
    ParallelEventProcessorOptions     m_options;
    std::vector<int>                  m_loader_ranks;
    std::vector<int>                  m_targets;
    std::unordered_set<ProductKey, ProductKey::hash> m_product_keys;

    tl::remote_procedure              m_req_events_rpc_rdma;
    tl::remote_procedure              m_req_events_rpc_no_rdma;
    std::vector<tl::provider_handle>  m_provider_handles;

    bool                              m_loader_running = false;
    std::queue<EventDescriptor>       m_event_queue;
    tl::mutex                         m_event_queue_mtx;
    tl::condition_variable            m_event_queue_cv;

    std::atomic<int>                  m_num_active_consumers;
    tl::eventual<void>                m_no_more_consumers;
    bool                              m_is_loader = false;

    size_t                            m_num_processing_ults = 0;
    tl::mutex                         m_processing_ults_mtx;
    tl::condition_variable            m_processing_ults_cv;

    tl::mutex                         m_stats_mtx;
    ParallelEventProcessorStatistics* m_stats = nullptr;

    ParallelEventProcessorImpl(
            std::shared_ptr<DataStoreImpl> ds,
            MPI_Comm comm,
            const ParallelEventProcessorOptions& options)
    : tl::provider<ParallelEventProcessorImpl>(ds->m_engine, options.providerID)
    , m_datastore(std::move(ds))
    , m_comm(comm)
    , m_options(options)
    , m_req_events_rpc_rdma(define("hepnos_pep_req_events_rdma", &ParallelEventProcessorImpl::requestEventsRDMA))
    , m_req_events_rpc_no_rdma(define("hepnos_pep_req_events_no_rdma", &ParallelEventProcessorImpl::requestEventsNoRDMA)) {
        spdlog::trace("Initializing ParallelEventProcessorImpl with provider id {}", options.providerID);
        int size;
        MPI_Comm_rank(comm, &m_my_rank);
        MPI_Comm_size(comm, &size);
        m_num_active_consumers = size-1;
        // exchange addresses
        std::string my_addr_str = m_datastore->m_engine.self();
        my_addr_str.resize(1024, '\0');
        std::vector<char> all_addresses_packed(size*1024);
        MPI_Allgather(my_addr_str.data(), 1024, MPI_BYTE, all_addresses_packed.data(), 1024, MPI_BYTE, comm);
        m_provider_handles.resize(size);
        for(unsigned i=0; i < size; i++) {
            unsigned j = (m_my_rank + i) % size;
            auto ep = m_datastore->m_engine.lookup(all_addresses_packed.data()+j*1024);
            m_provider_handles[j] = tl::provider_handle(std::move(ep), options.providerID);
        }
        spdlog::trace("ParallelEventProcessorImpl: address exchange done, found {} participants", size);
    }

    ~ParallelEventProcessorImpl() {
        m_req_events_rpc_rdma.deregister();
        m_req_events_rpc_no_rdma.deregister();
        spdlog::trace("ParallelEventProcessorImpl: finalizing");
    }

    /**
     * Main function to start processing events in parallel.
     */
    void process(const std::vector<EventSet>& evsets,
                 const ParallelEventProcessor::EventProcessingWithCacheFn& function,
                 ParallelEventProcessorStatistics* stats) {
        int size;
        MPI_Comm_size(m_comm, &size);
        m_is_loader = (m_loader_ranks[0] == m_my_rank);
        spdlog::trace("ParallelEventProcessorImpl: starting process, (this rank is {})",
                      m_is_loader ? "loader" : "not loader");
        if(size == 1)
            m_no_more_consumers.set_value();
        m_stats = stats;
        startLoadingEventsFromTargets(evsets);
        processEvents(function);
        m_stats = nullptr;
        if(m_is_loader) {
            spdlog::trace("ParallelEventProcessorImpl: waiting for all the consumers...");
            m_no_more_consumers.wait();
        }
        spdlog::trace("ParallelEventProcessorImpl: process completed");
    }

    /**
     * Starts the ULT that loads events from HEPnOS. This ULT is posted
     * on the first pool of the current ES.
     */
    void startLoadingEventsFromTargets(const std::vector<EventSet>& evsets) {
        if(evsets.size() == 0) {
            return;
        }
        spdlog::trace("ParallelEventProcessorImpl: starting ULT to load events");
        m_loader_running = true;
        tl::xstream::self().make_thread([this, evsets](){
            spdlog::trace("ParallelEventProcessorImpl: loader ULT started");
            loadEventsFromTargets(evsets);
            spdlog::trace("ParallelEventProcessorImpl: loader ULT completing");
        }, tl::anonymous());
    }

    /**
     * Content of the ULT that loads events from HEPnOS. This ULT
     * will loop over the EventSets, and inside an EventSet over the
     * Events, and push descriptors inside the event queue.
     */
    void loadEventsFromTargets(const std::vector<EventSet>& evsets) {
        for(auto& evset : evsets) {
            spdlog::trace("ParallelEventProcessorImpl: starting to load events from EventSet");
            Prefetcher prefetcher(
                    DataStore(m_datastore),
                    m_options.cacheSize,
                    m_options.inputBatchSize);
            // XXX instead of waking up all the threads at every event,
            // we should try to fill up batches of the appropriate size in the queue
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
        }
        spdlog::trace("ParallelEventProcessorImpl: no more events to load, notifying");
        m_event_queue_cv.notify_all();
    }

    void requestEventsRDMA(const tl::request& req, size_t max, tl::bulk& remote_mem) {
        spdlog::trace("ParallelEventProcessorImpl: (req={}) received request for up to {} events via RDMA",
                      (void*)(&req), max);
        std::vector<EventDescriptor> descriptorsToSend;
        descriptorsToSend.reserve(max);
        {
            std::unique_lock<tl::mutex> lock(m_event_queue_mtx);
            while(m_loader_running && m_event_queue.empty()) {
                spdlog::trace("ParallelEventProcessorImpl: (req={}) waiting for more events to be available...",
                              (void*)(&req));
                m_event_queue_cv.wait(lock);
            }
            for(unsigned i = 0; i < max && !m_event_queue.empty(); i++) {
                descriptorsToSend.push_back(m_event_queue.front());
                m_event_queue.pop();
            }
        }

        spdlog::trace("ParallelEventProcessorImpl: (req={}) sending {} events via RDMA",
                      (void*)(&req), descriptorsToSend.size());
        if(descriptorsToSend.size() != 0) {
            std::vector<std::pair<void*, size_t>> segment =
                {{ descriptorsToSend.data(), sizeof(descriptorsToSend[0])*descriptorsToSend.size() }};
            auto local_mem = m_datastore->m_engine.expose(segment, tl::bulk_mode::read_only);
            remote_mem.on(req.get_endpoint()) << local_mem;
        }

        spdlog::trace("ParallelEventProcessorImpl: (req={}) done sending",
                      (void*)(&req));
        req.respond(static_cast<size_t>(descriptorsToSend.size()));

        if(descriptorsToSend.empty()) {
            auto x = --m_num_active_consumers;
            if(x == 0) {
                m_no_more_consumers.set_value(); // allow the destructor to complete
            }
            spdlog::trace("ParallelEventProcessorImpl: (req={}) decreasing num consumers to {}",
                          (void*)(&req), x);
        }
    }

    void requestEventsNoRDMA(const tl::request& req, size_t max) {
        spdlog::trace("ParallelEventProcessorImpl: (req={}) received request for up to {} events via RPC",
                      (void*)(&req), max);
        std::vector<EventDescriptor> descriptorsToSend;
        descriptorsToSend.reserve(max);
        {
            std::unique_lock<tl::mutex> lock(m_event_queue_mtx);
            while(m_loader_running && m_event_queue.empty()) {
                spdlog::trace("ParallelEventProcessorImpl: (req={}) waiting for more events to be available...",
                              (void*)(&req));
                m_event_queue_cv.wait(lock);
            }
            for(unsigned i = 0; i < max && !m_event_queue.empty(); i++) {
                descriptorsToSend.push_back(m_event_queue.front());
                m_event_queue.pop();
            }
        }

        spdlog::trace("ParallelEventProcessorImpl: (req={}) sending {} events via RPC",
                      (void*)(&req), descriptorsToSend.size());
        req.respond(descriptorsToSend);
        spdlog::trace("ParallelEventProcessorImpl: (req={}) done sending",
                      (void*)(&req));

        if(descriptorsToSend.empty()) {
            auto x = --m_num_active_consumers;
            if(x == 0) {
                m_no_more_consumers.set_value(); // allow the destructor to complete
            }
            spdlog::trace("ParallelEventProcessorImpl: (req={}) decreasing num consumers to {}",
                          (void*)(&req), x);
        }
    }

    /**
     * This function tries to fill out the provided vector with a batch of
     * Event descriptors taken locally or from loader processes.
     * It returns true if new EventDescriptors were put in the vector,
     * false otherwise.
     */
    bool requestEvents(std::vector<EventDescriptor>& descriptors) {
        double t1 = tl::timer::wtime();
        double t2;
        spdlog::trace("Requesting batch of events");
        while(m_loader_ranks.size() != 0) {
            int loader_rank = m_loader_ranks[0];
            if(loader_rank == m_my_rank) {
                std::unique_lock<tl::mutex> lock(m_event_queue_mtx);
                while(m_event_queue.empty() && m_loader_running) {
                    spdlog::trace("Waiting for events to appear in local queue");
                    m_event_queue_cv.wait(lock);
                }
                size_t num_events_requested = m_options.outputBatchSize;
                descriptors.resize(num_events_requested);
                size_t num_actual_events = 0;
                for(unsigned i = 0; i < num_events_requested && !m_event_queue.empty(); i++) {
                    descriptors[i] = m_event_queue.front();
                    m_event_queue.pop();
                    num_actual_events += 1;
                    // no need to lock m_stats_mtx, this is the only ULT modifying local_events_processed
                    if(m_stats) {
                        m_stats->total_events_processed += 1;
                        m_stats->local_events_processed += 1;
                    }
                }
                if(num_actual_events != 0) {
                    spdlog::trace("Loaded {} events from local queue", num_actual_events);
                    descriptors.resize(num_actual_events);
                    t2 = tl::timer::wtime();
                    // no need to lock m_stats_mtx, this is the only ULT modifying waiting_time_stats
                    if(m_stats) m_stats->waiting_time_stats.updateWith(t2-t1);
                    return true;
                } else {
                    spdlog::trace("No more events in local queue, erasing self from loader ranks");
                    m_loader_ranks.erase(m_loader_ranks.begin());
                }
            } else {
                size_t max = m_options.outputBatchSize;
                if(!m_options.use_rdma) {
                    spdlog::trace("Loading events from loader rank {} via RPC", loader_rank);
                    descriptors = m_req_events_rpc_no_rdma
                        .on(m_provider_handles[loader_rank])(max).as<std::vector<EventDescriptor>>();
                } else {
                    spdlog::trace("Loading events from loader rank {} via RDMA", loader_rank);
                    descriptors.resize(max);
                    std::vector<std::pair<void*, size_t>> segment =
                        {{ descriptors.data(), sizeof(descriptors[0])*descriptors.size() }};
                    auto local_mem = m_datastore->m_engine.expose(segment, tl::bulk_mode::write_only);

                    size_t num_actual_events = m_req_events_rpc_rdma
                        .on(m_provider_handles[loader_rank])(max, local_mem);
                    descriptors.resize(num_actual_events);
                }
                spdlog::trace("Obtained {} events from loader rank {}", descriptors.size(), loader_rank);
                if(descriptors.size() != 0) {
                    t2 = tl::timer::wtime();
                    // no need to lock m_stats_mtx, this is the only ULT modifying waiting_time_stats
                    if(m_stats) {
                        m_stats->total_events_processed += descriptors.size();
                        m_stats->waiting_time_stats.updateWith(t2-t1);
                    }
                    return true;
                } else {
                    spdlog::trace("No more events in loader {}, erasing from loader ranks", loader_rank);
                    m_loader_ranks.erase(m_loader_ranks.begin());
                }
            }
        }
        t2 = tl::timer::wtime();
        spdlog::trace("No more events to load");
        // no need to lock m_stats_mtx, this is the only ULT modifying waiting_time_stats
        if(m_stats) m_stats->waiting_time_stats.updateWith(t2-t1);
        return false;
    }

    void preloadProductsForDescriptors(const std::vector<EventDescriptor>& descriptors,
                                       ProductCache& cache) {
        if(m_product_keys.size() == 0) return;
        spdlog::trace("Preloading products for {} events", descriptors.size());
        double t1 = tl::timer::wtime();

        size_t pks = 0;
        for(const auto& product_key : m_product_keys)
            pks += sizeof(EventDescriptor) + product_key.label.size() + 1 + product_key.type.size();

        // buffer for packed product ids
        std::string packed_product_ids;
        std::vector<ProductID> product_ids;
        packed_product_ids.reserve(descriptors.size() * pks);
        product_ids.reserve(descriptors.size());

        // size of each product id
        std::vector<size_t> packed_product_id_sizes;
        packed_product_id_sizes.reserve(descriptors.size());

        // size of each value
        std::vector<size_t> packed_value_sizes(descriptors.size(), 0);

        // build the list of packed product ids, product id sizes
        size_t offset = 0;
        size_t count = 0;
        long current_db_idx = -1;
        for(const auto& descriptor : descriptors) {
            // build a fake product id to get the db_index
            auto fake_product_id = DataStoreImpl::makeProductIDprefix(descriptor);
            auto db_idx = m_datastore->computeProductDbIndex(fake_product_id);

            // if the db_idx changed, we need to flush the current batch
            if(current_db_idx != -1 && current_db_idx != (long)db_idx) {

                spdlog::trace("Starting to preload products from database {}", current_db_idx);
                // get size of batch of products
                packed_value_sizes.resize(count);
                auto& db = m_datastore->getProductDatabase(current_db_idx);
                spdlog::trace("Getting {} product lengths from database", count);
                db.lengthPacked(count, packed_product_ids.data(),
                                packed_product_id_sizes.data(),
                                packed_value_sizes.data());
                spdlog::trace("Done getting {} product lengths from database", count);
                for(unsigned i=0; i < packed_value_sizes.size(); i++) {
                    auto s = packed_value_sizes[i];
                    if(s == YOKAN_KEY_NOT_FOUND) {
                        packed_value_sizes[i] = 0;
                    }
                }
                // allocate a buffer of appropriate size for packed values
                size_t buffer_size = std::accumulate(packed_value_sizes.begin(),
                                                     packed_value_sizes.end(), 0);
                if(buffer_size != 0) {
                    std::vector<char> value_buffer(buffer_size);
                    // get the actual values
                    spdlog::trace("Getting {} products from database", count);
                    db.getPacked(count, packed_product_ids.data(),
                                 packed_product_id_sizes.data(),
                                 buffer_size, value_buffer.data(),
                                 packed_value_sizes.data());
                    spdlog::trace("Done getting {} products from database", count);
                    // place data into cache
                    offset = 0;
                    for(unsigned i = 0; i < count; i++) {
                        if(packed_value_sizes[i] == YOKAN_KEY_NOT_FOUND) {
                            cache.m_impl->addNotFound(product_ids[i]);
                            continue;
                        }
                        if(packed_value_sizes[i] == YOKAN_SIZE_TOO_SMALL) {
                            spdlog::warn("A product (product_id = {}) "
                                    "could not be loaded because buffer is too small, "
                                    "which is not supposed to happen...", product_ids[i].toJSON());
                        }
                        if(packed_value_sizes[i] > YOKAN_LAST_VALID_SIZE)
                            break;
                        std::string data(value_buffer.data() + offset, packed_value_sizes[i]);
                        //spdlog::trace("Adding product to cache (product id = {})",
                        //              product_ids[i].toJSON());
                        cache.m_impl->addRawProduct(product_ids[i], std::move(data));
                        offset += packed_value_sizes[i];
                    }
                }
                // reset buffers and variables
                offset = 0;
                count = 0;
                product_ids.resize(0);
                packed_product_ids.resize(0);
                packed_product_id_sizes.resize(0);
                product_ids.reserve(descriptors.size());
                packed_product_ids.reserve(descriptors.size() & pks);
                packed_product_id_sizes.reserve(descriptors.size());
            }

            current_db_idx = db_idx;

            // go through all actual product keys
            for(const auto& product_key : m_product_keys) {
                auto product_id = DataStoreImpl::makeProductID(
                        descriptor, product_key.label.c_str(), product_key.label.size(),
                        product_key.type.c_str(), product_key.type.size());
                product_ids.push_back(product_id);
                auto key_size = product_id.m_key.size();
                packed_product_ids.resize(offset + key_size);
                std::memcpy(const_cast<char*>(packed_product_ids.data() + offset),
                            product_id.m_key.data(),
                            key_size);
                offset += key_size;
                packed_product_id_sizes.push_back(key_size);
                count += 1;
            }
        }

        if(current_db_idx != -1 && count != 0) {
            // get size of batch of products
            packed_value_sizes.resize(count);
            auto& db = m_datastore->getProductDatabase(current_db_idx);
            spdlog::trace("Getting {} product lengths from database", count);
            db.lengthPacked(count,
                    packed_product_ids.data(),
                    packed_product_id_sizes.data(),
                    packed_value_sizes.data());
            spdlog::trace("Done getting {} product lengths from database", count);
            // check the size of values
            for(unsigned i=0; i < packed_value_sizes.size(); i++) {
                auto s = packed_value_sizes[i];
                if(s == YOKAN_KEY_NOT_FOUND) {
                    packed_value_sizes[i] = 0;
                }
            }
            // allocate a buffer of appropriate size for packed values
            size_t buffer_size = std::accumulate(packed_value_sizes.begin(),
                    packed_value_sizes.end(), 0);
            if(buffer_size != 0) {
                std::vector<char> value_buffer(buffer_size);
                // get the actual values
                spdlog::trace("Getting {} products from database", count);
                db.getPacked(count, packed_product_ids.data(),
                        packed_product_id_sizes.data(),
                        buffer_size, value_buffer.data(),
                        packed_value_sizes.data());
                spdlog::trace("Done getting {} products from database", count);
                // place data into cache
                offset = 0;
                for(unsigned i = 0; i < count; i++) {
                    if(packed_value_sizes[i] == YOKAN_KEY_NOT_FOUND) {
                        cache.m_impl->addNotFound(product_ids[i]);
                        continue;
                    }
                    if(packed_value_sizes[i] == YOKAN_SIZE_TOO_SMALL) {
                        spdlog::warn("A product (product_id = {}) "
                            "could not be loaded because buffer is too small, "
                            "which is not supposed to happen...", product_ids[i].toJSON());
                    }
                    if(packed_value_sizes[i] > YOKAN_LAST_VALID_SIZE)
                        break;
                    std::string data(value_buffer.data() + offset, packed_value_sizes[i]);
                    //spdlog::trace("Adding product to cache (product_id = {})",
                    //              product_ids[i].toJSON());
                    cache.m_impl->addRawProduct(product_ids[i], std::move(data));
                    offset += packed_value_sizes[i];
                }
            }
        }

        spdlog::trace("Done preloading products");
        double t2 = tl::timer::wtime();
        if(m_stats) m_stats->acc_product_loading_time += t2-t1;
    }

    void processSingleEvent(const EventDescriptor& d,
                            const ParallelEventProcessor::EventProcessingWithCacheFn& user_function,
                            ProductCache& cache) {
        double t1, t2;
        t1 = tl::timer::wtime();
        Event event = Event::fromDescriptor(DataStore(m_datastore), d, false);
        user_function(event, cache);
        t2 = tl::timer::wtime();
        if(m_stats) {
            std::lock_guard<tl::mutex> lock(m_stats_mtx);
            m_stats->processing_time_stats.updateWith(t2-t1);
            m_stats->acc_event_processing_time += t2-t1;
        }
        if(m_async) {
            {
                std::unique_lock<tl::mutex> lock(m_processing_ults_mtx);
                m_num_processing_ults -= 1;
            }
            m_processing_ults_cv.notify_all();
        }
    }

    /**
     * This function keeps requesting new events and call the user-provided callback.
     */
    void processEvents(const ParallelEventProcessor::EventProcessingWithCacheFn& user_function) {
        spdlog::trace("Entering processEvents");
        if(m_stats) *m_stats = ParallelEventProcessorStatistics();
        double t_start = tl::timer::wtime();
        std::vector<EventDescriptor> descriptors;
        auto max_ults = m_async ? m_async->m_xstreams.size()*2 : 0;
        ProductCache cache{DataStore{m_datastore}};
        cache.m_impl->m_erase_on_load = true;
        while(requestEvents(descriptors)) {
            preloadProductsForDescriptors(descriptors, cache);
            for(auto& d : descriptors) {
                if(m_async) {
                    {   // don't submit more ULTs than twice the number of ES
                        std::unique_lock<tl::mutex> lock(m_processing_ults_mtx);
                        while(m_num_processing_ults >= max_ults) {
                            spdlog::trace("Waiting for some processing ULTs to complete...");
                            m_processing_ults_cv.wait(lock);
                        }
                        m_num_processing_ults += 1;
                    }
                    m_async->m_pool.make_thread([this, d, &cache, &user_function]() {
                        processSingleEvent(d, user_function, cache);
                    }, tl::anonymous());
                } else {
                    processSingleEvent(d, user_function, cache);
                }
            }
        }
        spdlog::trace("No more events to request");
        {   // wait until all ULTs completed
            std::unique_lock<tl::mutex> lock(m_processing_ults_mtx);
            while(m_num_processing_ults != 0) {
                spdlog::trace("Waiting for remaining processing ULTs to complete...");
                m_processing_ults_cv.wait(lock);
            }
        }
        spdlog::trace("Leaving processEvents");
        double t_end = tl::timer::wtime();
        if(m_stats) m_stats->total_time = t_end - t_start;
    }
};

}

#endif
