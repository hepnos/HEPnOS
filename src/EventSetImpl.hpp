/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRIVATE_EVENTSET_IMPL_H
#define __HEPNOS_PRIVATE_EVENTSET_IMPL_H

#include "hepnos/EventSet.hpp"
#include "hepnos/UUID.hpp"

namespace hepnos {

class DataStoreImpl;

class EventSetImpl {

    public:

        std::shared_ptr<DataStoreImpl>  m_datastore;
        uint8_t                         m_level;
        std::shared_ptr<std::string>    m_container;
        std::string                     m_name;
        UUID                            m_uuid;

        static EventSet::iterator m_end;

        EventSetImpl(const std::shared_ptr<DataStoreImpl>& ds,
             uint8_t level, 
             const std::shared_ptr<std::string>& container,
             const std::string& name,
             const UUID& uuid = UUID())
        : m_datastore(ds)
        , m_level(level)
        , m_container(container)
        , m_name(name)
        , m_uuid(uuid) {}

        EventSetImpl(const std::shared_ptr<DataStoreImpl>& ds,
             uint8_t level,
             const std::string& fullname,
             const UUID& uuid = UUID())
        : m_datastore(ds)
        , m_level(level)
        , m_uuid(uuid) {
            size_t p = fullname.find_last_of('/');
            m_name = fullname.substr(p+1);
            m_container = std::make_shared<std::string>(fullname.substr(0, p));
        }
};

}

#endif
