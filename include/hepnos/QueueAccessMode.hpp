/*
 * (C) 2022 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_QUEUE_ACCESS_MODE_HPP
#define __HEPNOS_QUEUE_ACCESS_MODE_HPP

namespace hepnos {

enum class QueueAccessMode : bool {
    CONSUMER = false,
    PRODUCER = true
};

}

#endif
