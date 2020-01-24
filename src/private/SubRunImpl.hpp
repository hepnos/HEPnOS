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

        static std::string makeKeyStringFromSubRunNumber(const SubRunNumber& n) {
            std::string str(1+sizeof(n),'\0');
            str[0] = '%';
#ifndef HEPNOS_READABLE_NUMBERS
#if BOOST_ENDIAN_BIG_BYTE
            std::memcpy(&str[1], &n, sizeof(n));
            return str;
#else
            unsigned i = sizeof(n);
            auto n2 = n;
            while(n2 != 0) {
                str[i] = n2 & 0xff;
                n2 = n2 >> 8;
                i -= 1;
            }
            return str;
#endif
#else
            std::stringstream strstr;
            strstr << "%" << std::setfill('0') << std::setw(16) << std::hex << n;
            return strstr.str();
#endif
        }

        static SubRunNumber parseSubRunNumberFromKeyString(const char* str) {
            if(str[0] != '%') return InvalidSubRunNumber;
            SubRunNumber n;
#ifdef HEPNOS_READABLE_NUMBERS
            std::stringstream strSubRunNumber;
            strSubRunNumber << std::hex << std::string(str+1, 16);
            strSubRunNumber >> n;
#else
#if BOOST_ENDIAN_BIG_BYTE
            std::memcpy(&n, &str[1], sizeof(n));
#else
            n = 0;
            for(unsigned i=0; i < sizeof(n); i++) {
                n = 256*n + str[i+1];
            }
#endif
#endif
            return n;
        }

        std::string makeKeyStringFromSubRunNumber() const {
            return makeKeyStringFromSubRunNumber(m_subrun_nr);
        }

        std::string fullpath() const {
            return *m_container + std::string("/") + makeKeyStringFromSubRunNumber();
        }
};

}

#endif
