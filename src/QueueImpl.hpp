/*
 * (C) 2022 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRIVATE_QUEUE_IMPL_H
#define __HEPNOS_PRIVATE_QUEUE_IMPL_H

#include <memory>
#include <string>
#include <thallium.hpp>
#include "hepnos/QueueAccessMode.hpp"

namespace hepnos {

class DataStoreImpl;

namespace tl = thallium;

class QueueImpl {

    public:

    std::shared_ptr<DataStoreImpl>               m_datastore;
    std::string                                  m_full_name;
    std::reference_wrapper<const std::type_info> m_type_info;
    QueueAccessMode                              m_mode;
    tl::provider_handle                          m_provider_handle;

    QueueImpl(const std::shared_ptr<DataStoreImpl>& ds,
              const std::string& full_name,
              const std::type_info& type_info,
              QueueAccessMode mode,
              tl::provider_handle ph)
    : m_datastore(ds)
    , m_full_name(full_name)
    , m_type_info(type_info)
    , m_mode(mode)
    , m_provider_handle(std::move(ph)) {}

};

}

#endif
