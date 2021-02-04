/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_STRING_HASH_HPP
#define __HEPNOS_STRING_HASH_HPP

namespace hepnos {

inline size_t hashString(const char* str, size_t size) {
    size_t hash = 14695981039346656037ULL;
    size_t prime = 1099511628211ULL;
    for(size_t i = 0; i < size; i++) {
        char c = str[i];
        hash = hash ^ c;
        hash *= prime;
    }
    return hash;
}

}

#endif
