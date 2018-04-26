/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_RUN_NUMBER_H
#define __HEPNOS_RUN_NUMBER_H

#include <cstdint>
#include <limits>

namespace hepnos {

typedef std::uint64_t RunNumber;

const RunNumber InvalidRunNumber = std::numeric_limits<RunNumber>::max();

}

#endif
