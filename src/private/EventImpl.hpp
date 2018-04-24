#ifndef __HEPNOS_PRIVATE_EVENT_IMPL_H
#define __HEPNOS_PRIVATE_EVENT_IMPL_H

#include <sstream>
#include <iomanip>
#include "hepnos/Event.hpp"

namespace hepnos {

class Event::Impl {

    public:

        DataStore*   m_datastore;
        uint8_t      m_level;
        std::string  m_container;
        EventNumber  m_event_nr;

        Impl(DataStore* ds, uint8_t level, const std::string& container, const EventNumber& n)
        : m_datastore(ds)
        , m_level(level)
        , m_container(container)
        , m_event_nr(n) {}

        static std::string makeKeyStringFromEventNumber(const EventNumber& n) {
            std::stringstream strstr;
            strstr << "%" << std::setfill('0') << std::setw(16) << std::hex << n;
            return strstr.str();
        }

        std::string makeKeyStringFromEventNumber() const {
            return makeKeyStringFromEventNumber(m_event_nr);
        }

        std::string fullpath() const {
            return m_container + std::string("/") + makeKeyStringFromEventNumber();
        }
};

}

#endif
