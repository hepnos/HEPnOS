#include "hepnos/DataSet.hpp"

namespace hepnos {

DataSet::DataSet(DataStore& ds, uint8_t level, const std::string& name) 
: m_datastore(ds)
, m_level(level)
, m_name(name) {}

}
