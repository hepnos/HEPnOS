/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_EVENT_NUMBER_H
#define __HEPNOS_EVENT_NUMBER_H

#include <cstdint>
#include <limits>

namespace hepnos {

typedef std::uint64_t EventNumber;

const EventNumber InvalidEventNumber = std::numeric_limits<EventNumber>::max();

}

#endif
