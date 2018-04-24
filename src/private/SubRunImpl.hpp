#ifndef __HEPNOS_PRIVATE_SUBRUN_IMPL_H
#define __HEPNOS_PRIVATE_SUBRUN_IMPL_H

#include <sstream>
#include <iomanip>
#include "hepnos/SubRun.hpp"

namespace hepnos {

class SubRun::Impl {

    public:

        DataStore*   m_datastore;
        uint8_t      m_level;
        std::string  m_container;
        SubRunNumber m_subrun_nr;
        iterator     m_end;

        Impl(DataStore* ds, uint8_t level, const std::string& container, const SubRunNumber& rn)
        : m_datastore(ds)
        , m_level(level)
        , m_container(container)
        , m_subrun_nr(rn) {}

        static std::string makeKeyStringFromSubRunNumber(const SubRunNumber& n) {
            std::stringstream strstr;
            strstr << "%" << std::setfill('0') << std::setw(16) << std::hex << n;
            return strstr.str();
        }

        std::string makeKeyStringFromSubRunNumber() const {
            return makeKeyStringFromSubRunNumber(m_subrun_nr);
        }

        std::string fullpath() const {
            return m_container + std::string("/") + makeKeyStringFromSubRunNumber();
        }
};

}

#endif
