/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRIVATE_SUBRUN_IMPL_H
#define __HEPNOS_PRIVATE_SUBRUN_IMPL_H

#include <sstream>
#include <memory>
#include <iomanip>
#include "DataStoreImpl.hpp"
#include "hepnos/Run.hpp"
#include "hepnos/SubRun.hpp"
#include "NumberUtil.hpp"

namespace hepnos {

class SubRunImpl {

    public:

        std::shared_ptr<DataStoreImpl> m_datastore;
        uint8_t                        m_level;
        std::shared_ptr<std::string>   m_dataset_name;
        RunNumber                      m_run_number;
        SubRunNumber                   m_subrun_number;
        
        static SubRun::iterator m_end;

        SubRunImpl(const std::shared_ptr<DataStoreImpl>& ds,
             uint8_t level, 
             const std::shared_ptr<std::string>& dataset,
             const RunNumber& rn, const SubRunNumber& srn)
        : m_datastore(ds)
        , m_level(level)
        , m_dataset_name(dataset)
        , m_run_number(rn)
        , m_subrun_number(srn) {}

        std::string makeKeyStringFromSubRunNumber() const {
            return makeKeyStringFromNumber(m_subrun_number);
        }

        std::string container() const {
            return *m_dataset_name + "/" + makeKeyStringFromNumber(m_run_number);
        }

        std::string fullpath() const {
            return container() + "/" + makeKeyStringFromSubRunNumber();
        }
};

}

#endif
