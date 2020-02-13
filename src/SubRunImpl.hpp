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
#include "RunImpl.hpp"
#include "NumberUtil.hpp"

namespace hepnos {

class SubRunImpl {

    public:

        std::shared_ptr<DataStoreImpl> m_datastore;
        std::shared_ptr<RunImpl>       m_run;
        SubRunNumber                   m_subrun_number;
        uint8_t                        m_level;
        
        static SubRun::iterator m_end;

        SubRunImpl(uint8_t level, 
                   const std::shared_ptr<RunImpl>& run,
                   const SubRunNumber& srn)
        : m_run(run)
        , m_subrun_number(srn)
        , m_level(level) {
            if(m_run) m_datastore = m_run->m_datastore;
        }

        bool operator==(const SubRunImpl& other) const {
            if(m_subrun_number != other.m_subrun_number) return false;
            if(m_run == other.m_run) return true;
            return *m_run == *other.m_run;
        }

        std::string makeKeyStringFromSubRunNumber() const {
            return makeKeyStringFromNumber(m_subrun_number);
        }

        std::string container() const {
            return m_run->fullpath();
        }

        std::string fullpath() const {
            return container() + "/" + makeKeyStringFromSubRunNumber();
        }
};

}

#endif
