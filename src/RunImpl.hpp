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
#include "DataStoreImpl.hpp"
#include "hepnos/Run.hpp"
#include "NumberUtil.hpp"

namespace hepnos {

class RunImpl {

    public:

        std::shared_ptr<DataStoreImpl> m_datastore;
        uint8_t                        m_level;
        UUID                           m_dataset_uuid;
        RunNumber                      m_run_number;

        static Run::iterator m_end;

        RunImpl(const std::shared_ptr<DataStoreImpl>& ds,
             uint8_t level,
             const UUID& uuid,
             const RunNumber& rn)
        : m_datastore(ds)
        , m_level(level)
        , m_dataset_uuid(uuid)
        , m_run_number(rn) {}

        bool operator==(const RunImpl& other) const {
            return (m_run_number == other.m_run_number)
                && (m_dataset_uuid == other.m_dataset_uuid);
        }

        std::string makeKeyStringFromRunNumber() const {
            return makeKeyStringFromNumber(m_run_number);
        }

        std::string container() const {
            return m_dataset_uuid.to_string();
        }

        std::string fullpath() const {
            return container() + "/" + makeKeyStringFromRunNumber();
        }
};

}

#endif
