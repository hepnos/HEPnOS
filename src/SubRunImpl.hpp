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
#include "hepnos/SubRun.hpp"
#include "NumberUtil.hpp"

namespace hepnos {

class SubRun::Impl {

    public:

        DataStore*   m_datastore;
        uint8_t      m_level;
        std::shared_ptr<std::string>  m_container;
        SubRunNumber m_subrun_nr;
        iterator     m_end;

        Impl(DataStore* ds, uint8_t level, const std::shared_ptr<std::string>& container, const SubRunNumber& rn)
        : m_datastore(ds)
        , m_level(level)
        , m_container(container)
        , m_subrun_nr(rn) {}

        std::string makeKeyStringFromSubRunNumber() const {
            return makeKeyStringFromNumber(m_subrun_nr);
        }

        std::string fullpath() const {
            return *m_container + std::string("/") + makeKeyStringFromSubRunNumber();
        }
};

}

#endif
