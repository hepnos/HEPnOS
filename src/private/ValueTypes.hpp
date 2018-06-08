/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __PRIVATE_VALUE_TYPES_H
#define __PRIVATE_VALUE_TYPES_H

#include <cstring>
#include <cstdlib>
#include <cstdint>
#include <memory>

namespace hepnos {

class DataStoreValue {

    size_t           m_object_size;
    uint64_t         m_server_id;
    bake_region_id_t m_region_id;

    public:

    DataStoreValue()
    : m_object_size(0), m_server_id(0) {}

    DataStoreValue(size_t object_size, uint64_t bake_server_id, const bake_region_id_t& region_id)
    : m_object_size(object_size), m_server_id(bake_server_id), m_region_id(region_id) {}

    size_t getDataSize() const {
        return m_object_size;
    }

    const bake_region_id_t& getBakeRegionID() const {
        return m_region_id;
    }

    const uint64_t& getBakeServerID() const {
        return m_server_id;
    }
};

}

#endif
