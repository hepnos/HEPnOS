/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRIVATE_EVENTSET_IMPL_H
#define __HEPNOS_PRIVATE_EVENTSET_IMPL_H

#include "hepnos/EventSet.hpp"
#include "DataSetImpl.hpp"

namespace hepnos {

class DataStoreImpl;

class EventSetImpl : public DataSetImpl {

        public:

        const int m_target; // target to which to restrict the EventSet, or -1 if no restriction
        const int m_num_targets; // number of targets; copy of datastore->numTargets(ItemType::EVENT)

        EventSetImpl(const DataSetImpl& dataset, int target=-1, int num_targets=0)
        : DataSetImpl(dataset)
        , m_target(target)
        , m_num_targets(num_targets)
        {}
};

}

#endif
