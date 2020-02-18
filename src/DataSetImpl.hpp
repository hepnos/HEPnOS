/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRIVATE_DATASET_IMPL_H
#define __HEPNOS_PRIVATE_DATASET_IMPL_H

#include "hepnos/DataSet.hpp"
#include "hepnos/RunSet.hpp"

namespace hepnos {

class DataStoreImpl;

class DataSetImpl {

    public:

        std::shared_ptr<DataStoreImpl>  m_datastore;
        uint8_t                         m_level;
        std::shared_ptr<std::string>    m_container;
        std::string                     m_name;

        static DataSet::iterator m_end;

        DataSetImpl(const std::shared_ptr<DataStoreImpl>& ds,
             uint8_t level, 
             const std::shared_ptr<std::string>& container,
             const std::string& name)
        : m_datastore(ds)
        , m_level(level)
        , m_container(container)
        , m_name(name) {}

        DataSetImpl(const std::shared_ptr<DataStoreImpl>& ds,
             uint8_t level,
             const std::string& fullname)
        : m_datastore(ds)
        , m_level(level) {
            size_t p = fullname.find_last_of('/');
            m_name = fullname.substr(p+1);
            m_container = std::make_shared<std::string>(fullname.substr(0, p));
        }

        std::string fullname() const {
            auto result = *m_container;
            if(m_name.size() > 0)
                result += "/" + m_name; // we do this because the root dataset has m_name == ""
            return result;
        }
};

}

#endif
