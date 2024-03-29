/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <spdlog/spdlog.h>
#include "hepnos/ParallelEventProcessor.hpp"
#include "hepnos/AsyncEngine.hpp"
#include "ParallelEventProcessorImpl.hpp"
#include "AsyncEngineImpl.hpp"

namespace hepnos {

ParallelEventProcessor::ParallelEventProcessor(
        const DataStore& datastore,
        MPI_Comm comm,
        const ParallelEventProcessorOptions& options)
: m_impl(std::make_shared<ParallelEventProcessorImpl>(datastore.m_impl, comm, options))
{
    int num_procs;
    int my_rank;
    MPI_Comm_rank(comm, &my_rank);
    MPI_Comm_size(comm, &num_procs);
    int num_targets = datastore.numTargets(ItemType::EVENT);
    std::vector<int> loader_ranks; // ranks that will load events
    std::vector<int> my_targets; // targets that this rank load from
    if(num_targets >= num_procs) {
        for(unsigned i=0; i < num_procs; i++) {
            loader_ranks.push_back(i);
        }
        for(unsigned i=0; i < num_targets; i++) {
            if(my_rank == i % num_procs) {
                my_targets.push_back(i);
            }
        }
    } else {
        const unsigned x = num_procs % num_targets == 0 ?
              num_procs/num_targets
            : num_procs/num_targets + 1;
        unsigned j = 0;
        bool stop_adding_to_loader_ranks = false;
        for(unsigned i = 0; i < num_targets; i++) {
            if(!stop_adding_to_loader_ranks)
                loader_ranks.push_back(j);
            if(j == my_rank) {
                my_targets.push_back(i);
            }
            j += x;
            if(j >= num_procs) {
                stop_adding_to_loader_ranks = true;
                j = 0;
            }
        }
    }
    // we want loader_ranks to start with the first rank greater or equal to my_rank
    auto ub = std::lower_bound(loader_ranks.begin(), loader_ranks.end(), my_rank);
    std::rotate(loader_ranks.begin(), ub, loader_ranks.end());

    m_impl->m_loader_ranks = std::move(loader_ranks);
    m_impl->m_targets = std::move(my_targets);
    spdlog::trace("Initialized ParallelEventProcessor with {} loader ranks and {} local targets",
                  m_impl->m_loader_ranks.size(), m_impl->m_targets.size());
    MPI_Barrier(comm);
}

ParallelEventProcessor::ParallelEventProcessor(
        const AsyncEngine& async,
        MPI_Comm comm,
        const ParallelEventProcessorOptions& options)
: ParallelEventProcessor(DataStore(async.m_impl->m_datastore), comm, options) {
    m_impl->m_async = async.m_impl;
    spdlog::trace("ParallelEventProcessor will use provided AsyncEngine");
}

ParallelEventProcessor::~ParallelEventProcessor() {
    spdlog::trace("Finalizing ParallelEventProcessor");
}

void ParallelEventProcessor::process(
        const DataSet& dataset,
        const EventProcessingWithCacheFn& function,
        ParallelEventProcessorStatistics* stats) {
    // make sure this function is called on the same dataset by all the processes
    UUID dsetid = dataset.m_impl->m_uuid;
    UUID uuid;
    MPI_Allreduce(&dsetid, &uuid, sizeof(uuid), MPI_BYTE, MPI_BAND, m_impl->m_comm);
    char c = dsetid == uuid ? 1 : 0;
    char c2;
    MPI_Allreduce(&c, &c2, 1, MPI_BYTE, MPI_BAND, m_impl->m_comm);
    if(!c2) {
        throw Exception("ParallelEventProcessor::process called on different DataSets by distinct processes");
    }
    // get the event sets
    std::vector<EventSet> ev_sets;
    for(auto t : m_impl->m_targets) {
        ev_sets.push_back(dataset.events(t));
    }
    spdlog::trace("ParallelEventProcessing: started processing, {} local EventSet found", ev_sets.size());
    m_impl->process(ev_sets, function, stats);
    spdlog::trace("ParallelEventProcessing: done processing");
}

void ParallelEventProcessor::preloadImpl(std::string label, std::string type) {
    m_impl->m_product_keys.emplace(std::move(label), std::move(type));
}

}
