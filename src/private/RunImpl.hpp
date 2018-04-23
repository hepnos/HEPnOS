#ifndef __HEPNOS_PRIVATE_RUN_IMPL_H
#define __HEPNOS_PRIVATE_RUN_IMPL_H

#include <sstream>
#include <iomanip>
#include "hepnos/Run.hpp"

namespace hepnos {

class Run::Impl {

    public:

        DataStore*   m_datastore;
        uint8_t      m_level;
        std::string  m_container;
        RunNumber    m_run_nr;

        Impl(DataStore* ds, uint8_t level, const std::string& container, const RunNumber& rn)
        : m_datastore(ds)
        , m_level(level)
        , m_container(container)
        , m_run_nr(rn) {}

        static std::string makeKeyStringFromRunNumber(const RunNumber& nr) {
            std::stringstream strstr;
            strstr << "%" << std::setfill('0') << std::setw(16) << std::hex << nr;
            return strstr.str();
        }

        std::string makeKeyStringFromRunNumber() const {
            return makeKeyStringFromRunNumber(m_run_nr);
        }

        std::string fullpath() const {
            return m_container + std::string("/") + makeKeyStringFromRunNumber(m_run_nr);
        }
};

}

#endif
