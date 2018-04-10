#ifndef __HEPNOS_DATA_SET_H
#define __HEPNOS_DATA_SET_H

#include <hepnos/DataStore.hpp>

namespace hepnos {

class DataSet {

    friend class DataStore;

    private:

        DataSet(DataStore& ds, uint8_t level, const std::string& name);

        DataStore&  m_datastore;
        uint8_t     m_level;
        std::string m_name;
};

}

#endif
