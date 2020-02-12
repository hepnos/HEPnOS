/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRIVATE_NUMBER_UTIL_H
#define __HEPNOS_PRIVATE_NUMBER_UTIL_H

#include <sstream>
#include <memory>
#include <iomanip>

namespace hepnos {

template<typename T>
static std::string makeReadableKeyStringFromNumber(const T& n) {
    constexpr int s = sizeof(T)*2;
    std::stringstream strstr;
    strstr << '%' << std::setfill('0') << std::setw(s) << std::hex << n;
    return strstr.str();
}

template<typename T>
static std::string makeKeyStringFromNumber(const T& n) {
#ifdef HEPNOS_READABLE_NUMBERS
        return makeReadableKeyStringFromNumber(n);
#else
        std::string str(1+sizeof(n),'\0');
        str[0] = '%';
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
#endif
}

template<typename T>
static T parseNumberFromKeyString(const char* str) {
    // string is assumed to start with a '%'
    T n;
#ifdef HEPNOS_READABLE_NUMBERS
    std::stringstream strNumber;
    strNumber << std::hex << std::string(str+1, sizeof(T)*2);
    strNumber >> n;
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

}

#endif
