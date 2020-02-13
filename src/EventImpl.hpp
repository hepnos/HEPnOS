/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRIVATE_EVENT_IMPL_H
#define __HEPNOS_PRIVATE_EVENT_IMPL_H

#include <boost/predef/other/endian.h>
#include <sstream>
#include <iomanip>
#include <memory>
#include "DataStoreImpl.hpp"
#include "hepnos/Run.hpp"
#include "hepnos/SubRun.hpp"
#include "hepnos/Event.hpp"
#include "NumberUtil.hpp"

namespace hepnos {

class Event::Impl {

    public:

        std::shared_ptr<DataStore::Impl> m_datastore;
        uint8_t                          m_level;
        std::shared_ptr<std::string>     m_dataset_name;
        RunNumber                        m_run_number;
        SubRunNumber                     m_subrun_number;
        EventNumber                      m_event_number;

        Impl(const std::shared_ptr<DataStore::Impl>& ds,
             uint8_t level,
             const std::shared_ptr<std::string>& dataset,
             const RunNumber& rn,
             const SubRunNumber& srn,
             const EventNumber& evn)
        : m_datastore(ds)
        , m_level(level)
        , m_dataset_name(dataset)
        , m_run_number(rn)
        , m_subrun_number(srn)
        , m_event_number(evn) {}

        std::string makeKeyStringFromEventNumber() const {
            return makeKeyStringFromNumber(m_event_number);
        }

        std::string container() const {
            return *m_dataset_name + "/" 
                + makeKeyStringFromNumber(m_run_number) + "/"
                + makeKeyStringFromNumber(m_subrun_number);
        }

        std::string fullpath() const {
            return container() + "/" + makeKeyStringFromEventNumber();
        }
};

}

#endif
