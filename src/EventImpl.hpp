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
#include "SubRunImpl.hpp"

namespace hepnos {

class EventImpl {

    public:

        std::shared_ptr<DataStoreImpl> m_datastore;
        std::shared_ptr<SubRunImpl>    m_subrun;
        EventNumber                    m_event_number;
        uint8_t                        m_level;

        EventImpl(uint8_t level,
                  const std::shared_ptr<SubRunImpl>& subrun,
                  const EventNumber& evn)
        : m_subrun(subrun)
        , m_event_number(evn)
        , m_level(level) {
            if(subrun) m_datastore = subrun->m_datastore;
        }

        bool operator==(const EventImpl& other) const {
            if(m_event_number != other.m_event_number) return false;
            if(m_subrun == other.m_subrun) return true;
            return *m_subrun == *other.m_subrun;
        }

        std::string makeKeyStringFromEventNumber() const {
            return makeKeyStringFromNumber(m_event_number);
        }

        std::string container() const {
            return m_subrun->fullpath();
        }

        std::string fullpath() const {
            return container() + "/" + makeKeyStringFromEventNumber();
        }
};

}

#endif
