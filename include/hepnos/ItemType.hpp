/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_ITEM_TYPE_HPP
#define __HEPNOS_ITEM_TYPE_HPP

namespace hepnos {

/**
 * @brief Type of item (DataSet, Run, SubRun, Event, or Product).
 */
enum class ItemType : uint32_t {
    DATASET,
    RUN,
    SUBRUN,
    EVENT,
    PRODUCT
};

}

#endif
