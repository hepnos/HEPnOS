/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_STRING_HASH_HPP
#define __HEPNOS_STRING_HASH_HPP

namespace hepnos {

inline size_t hashString(const std::string& str) {
    size_t hash = 14695981039346656037ULL;
    size_t prime = 1099511628211ULL;
    for(const auto& c : str) {
        hash = hash ^ c;
        hash *= prime;
    }
    return hash;
}

}

#endif
