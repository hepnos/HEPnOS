/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "hepnos/ParallelEventProcessor.hpp"
#include "ParallelEventProcessorImpl.hpp"

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
        for(unsigned i = 0; i < num_targets; i++) {
            loader_ranks.push_back(j);
            if(j == my_rank) {
                my_targets.push_back(i);
            }
            j += x;
            j %= num_procs;
        }
    }
    // we want loader_ranks to start with the first rank greater or equal to my_rank
    auto ub = std::lower_bound(loader_ranks.begin(), loader_ranks.end(), my_rank);
    std::rotate(loader_ranks.begin(), ub, loader_ranks.end());

    m_impl->m_loader_ranks = std::move(loader_ranks);
    m_impl->m_targets = std::move(my_targets);
}

ParallelEventProcessor::~ParallelEventProcessor() {
    if(m_impl) {
        MPI_Barrier(m_impl->m_comm);
    }
}

void ParallelEventProcessor::process(
        const DataSet& dataset,
        const EventProcessingWithCacheFn& function,
        ParallelEventProcessorStatistics* stats) {
    std::vector<EventSet> ev_sets;
    for(auto t : m_impl->m_targets) {
        ev_sets.push_back(dataset.events(t));
    }
    m_impl->process(ev_sets, function, stats);
}

void ParallelEventProcessor::preloadImpl(const std::string& productKey) {
    m_impl->m_product_keys.insert(productKey);
}

}
