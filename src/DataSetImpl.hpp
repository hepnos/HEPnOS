/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRIVATE_DATASET_IMPL_H
#define __HEPNOS_PRIVATE_DATASET_IMPL_H

#include "hepnos/DataSet.hpp"

namespace hepnos {

class DataSet::Impl {

    public:

        DataStore*   m_datastore;
        uint8_t      m_level;
        std::shared_ptr<std::string>  m_container;
        std::string  m_name;
        RunSet       m_runset;

        Impl(DataSet* dataset, DataStore* ds, uint8_t level, 
                const std::shared_ptr<std::string>& container, const std::string& name)
        : m_datastore(ds)
        , m_level(level)
        , m_container(container)
        , m_name(name)
        , m_runset(dataset) {}
};

}

#endif
