/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRIVATE_RUN_IMPL_H
#define __HEPNOS_PRIVATE_RUN_IMPL_H

#include <sstream>
#include <iomanip>
#include <memory>
#include "hepnos/Run.hpp"
#include "NumberUtil.hpp"

namespace hepnos {

class Run::Impl {

    public:

        DataStore*                    m_datastore;
        uint8_t                       m_level;
        std::shared_ptr<std::string>  m_dataset_name;
        RunNumber                     m_run_number;

        static iterator m_end;

        Impl(DataStore* ds, uint8_t level,
             const std::shared_ptr<std::string>& dataset,
             const RunNumber& rn)
        : m_datastore(ds)
        , m_level(level)
        , m_dataset_name(dataset)
        , m_run_number(rn) {}

        std::string makeKeyStringFromRunNumber() const {
            return makeKeyStringFromNumber(m_run_number);
        }

        std::string fullpath() const {
            return *m_dataset_name + std::string("/") + makeKeyStringFromRunNumber();
        }
};

}

#endif
