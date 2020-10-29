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
#include "hepnos/EventSet.hpp"
#include "hepnos/ParallelEventProcessor.hpp"

namespace hepnos {

namespace tl = thallium;

typedef std::vector<EventDescriptor> DescriptorBatch;

struct ParallelEventProcessorImpl {

    std::shared_ptr<DataStoreImpl> m_datastore;
    MPI_Comm                       m_comm;
    Prefetcher                     m_prefetcher;
    DispatchPolicy                 m_policy;
    std::vector<int>               m_loader_ranks;
    std::vector<int>               m_targets;

    bool                           m_loader_running = false;
    std::queue<EventDescriptor>    m_event_queue;
    tl::mutex                      m_event_queue_mtx;
    tl::condition_variable         m_event_queue_cv;

    int                            m_num_active_consumers;
    tl::managed<tl::xstream>       m_mpi_xstream;

    ParallelEventProcessorStatistics* m_stats = nullptr;

    ParallelEventProcessorImpl(
            std::shared_ptr<DataStoreImpl> ds,
            MPI_Comm comm,
            const Prefetcher& prefetcher,
            const DispatchPolicy& policy)
    : m_datastore(std::move(ds))
    , m_comm(comm)
    , m_prefetcher(prefetcher)
    , m_policy(policy) {
        MPI_Comm_size(comm, &m_num_active_consumers);
        m_num_active_consumers -= 1;
    }

    ~ParallelEventProcessorImpl() {}

    void process(const std::vector<EventSet>& evsets,
                 const ParallelEventProcessor::EventProcessingFn& function,
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

    void startLoadingEventsFromTargets(const std::vector<EventSet>& evsets) {
        if(evsets.size() == 0) {
            return;
        }
        m_loader_running = true;
        tl::xstream::self().make_thread([this, evsets](){
            loadEventsFromTargets(evsets);
        }, tl::anonymous());
    }

    void loadEventsFromTargets(const std::vector<EventSet>& evsets) {
        for(auto& evset : evsets) {
            for(auto it = evset.begin(m_prefetcher); it != evset.end(); it++) {
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

    void startRespondingToMPIrequests() {
        if(!m_loader_running)
            return;
        m_mpi_xstream = tl::xstream::create();
        m_mpi_xstream->make_thread([this](){
            respondToMPIrequests();
        }, tl::anonymous());
    }

    void respondToMPIrequests() {
        int my_rank;
        MPI_Comm_rank(m_comm, &my_rank);
        while(m_num_active_consumers != 0) {
            MPI_Status status;
            MPI_Recv(NULL, 0, MPI_BYTE, MPI_ANY_SOURCE, 1111, m_comm, &status);
            int consumer_rank = status.MPI_SOURCE;
            bool shouldSendEvent;
            EventDescriptor descriptorToSend;
            {
                std::unique_lock<tl::mutex> lock(m_event_queue_mtx);
                while(m_loader_running && m_event_queue.size() == 0)
                     m_event_queue_cv.wait(lock);
                if(m_event_queue.size() > 0) {
                    descriptorToSend = m_event_queue.front();
                    m_event_queue.pop();
                    shouldSendEvent = true;
                } else {
                    shouldSendEvent = false;
                }
            }
            if(shouldSendEvent) {
                MPI_Send(&descriptorToSend, sizeof(descriptorToSend), MPI_BYTE, consumer_rank, 1112, m_comm);
            } else {
                MPI_Send(NULL, 0, MPI_BYTE, consumer_rank, 1112, m_comm);
                m_num_active_consumers -= 1;
            }
        }
    }

    bool requestEvents(EventDescriptor& descriptor) {
        int my_rank;
        MPI_Comm_rank(m_comm, &my_rank);
        while(m_loader_ranks.size() != 0) {
            int loader_rank = m_loader_ranks[0];
            if(loader_rank == my_rank) {
                std::unique_lock<tl::mutex> lock(m_event_queue_mtx);
                while(m_event_queue.size() == 0 && m_loader_running)
                     m_event_queue_cv.wait(lock);
                if(m_event_queue.size() > 0) {
                    descriptor = m_event_queue.front();
                    m_event_queue.pop();
                    if(m_stats) m_stats->local_events_processed += 1;
                    return true;
                } else {
                    m_loader_ranks.erase(m_loader_ranks.begin());
                }
            } else {
                MPI_Send(NULL, 0, MPI_BYTE, loader_rank, 1111, m_comm);
                MPI_Status status;
                MPI_Recv(&descriptor, sizeof(descriptor), MPI_BYTE, loader_rank, 1112, m_comm, &status);
                int count;
                MPI_Get_count(&status, MPI_BYTE, &count);
                if(count == sizeof(descriptor)) {
                    return true;
                } else {
                    m_loader_ranks.erase(m_loader_ranks.begin());
                }
            }
        }
        return false;
    }

    void processEvents(const ParallelEventProcessor::EventProcessingFn& user_function) {
        if(m_stats) *m_stats = ParallelEventProcessorStatistics();
        double t_start = tl::timer::wtime();
        EventDescriptor descriptor;
        double t1;
        double t2 = tl::timer::wtime();
        while(requestEvents(descriptor)) {
            Event event = Event::fromDescriptor(DataStore(m_datastore), descriptor, false);
            t1 = tl::timer::wtime();
            if(m_stats) m_stats->waiting_time_stats.updateWith(t1-t2);
            user_function(event);
            t2 = tl::timer::wtime();
            if(m_stats) {
                m_stats->processing_time_stats.updateWith(t2 - t1);
                m_stats->total_processing_time += t2 - t1;
                m_stats->total_events_processed += 1;
            }
        }
        double t_end = tl::timer::wtime();
        if(m_stats) m_stats->total_time = t_end - t_start;
    }
};

}

#endif
