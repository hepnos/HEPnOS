/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "hepnos/ProductID.hpp"

namespace hepnos {

void ProductID::unpackInformation(UUID* dataset_id,
                           RunNumber* run,
                           SubRunNumber* subrun,
                           EventNumber* event,
                           std::string* label,
                           std::string* type) const {

    if(m_key.empty()) return;
    size_t s = 0;
    if(dataset_id)
        std::memcpy(dataset_id, m_key.data()+s, sizeof(*dataset_id));
    s += sizeof(*dataset_id);
    if(run)
        std::memcpy(run, m_key.data()+s, sizeof(*run));
    s += sizeof(*run);
    if(subrun)
        std::memcpy(subrun, m_key.data()+s, sizeof(*subrun));
    s += sizeof(*subrun);
    if(event)
        std::memcpy(event, m_key.data()+s, sizeof(*event));
    s += sizeof(*event);
    auto name = m_key.substr(s);
    auto p = name.find('#');
    if(p == std::string::npos)
        return;
    if(label) *label = name.substr(0,p);
    if(type) *type = name.substr(p+1);
}

}
