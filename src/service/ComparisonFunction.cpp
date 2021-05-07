/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "ComparisonFunction.hpp"

extern "C" int hepnos_compare_item_descriptors(const void* id1_ptr, hg_size_t size1, const void* id2_ptr, hg_size_t size2) {
    const hepnos::ItemDescriptor* id1 = reinterpret_cast<const hepnos::ItemDescriptor*>(id1_ptr);
    const hepnos::ItemDescriptor* id2 = reinterpret_cast<const hepnos::ItemDescriptor*>(id2_ptr);
    if(*id1 == *id2) return 0;
    if(*id1 < *id2) return -1;
    return 1;
}
