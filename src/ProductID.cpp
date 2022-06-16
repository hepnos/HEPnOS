/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "hepnos/ProductID.hpp"
#include "hepnos/BigEndian.hpp"
#include <sstream>

namespace hepnos {

bool ProductID::unpackInformation(UUID* dataset_id,
                                  RunNumber* run,
                                  SubRunNumber* subrun,
                                  EventNumber* event,
                                  std::string* label,
                                  std::string* type) const {

    if(m_key.empty()) return false;
    size_t s = 0;
    if(dataset_id)
        std::memcpy(dataset_id, m_key.data()+s, sizeof(*dataset_id));
    s += sizeof(*dataset_id);
    BE<RunNumber>    be_run;
    BE<SubRunNumber> be_subrun;
    BE<EventNumber>  be_event;
    std::memcpy(&be_run, m_key.data()+s, sizeof(be_run));
    s += sizeof(be_run);
    std::memcpy(&be_subrun, m_key.data()+s, sizeof(be_subrun));
    s += sizeof(be_subrun);
    std::memcpy(&be_event, m_key.data()+s, sizeof(be_event));
    s += sizeof(be_event);
    if(run) *run = be_run;
    if(subrun) *subrun = be_subrun;
    if(event) *event = be_event;
    auto name = m_key.substr(s);
    auto p = name.find('#');
    if(p == std::string::npos)
        return true;
    if(label) *label = name.substr(0,p);
    if(type) *type = name.substr(p+1);
    return true;
}

std::string ProductID::toJSON() const {
    UUID dataset_id;
    RunNumber run = InvalidRunNumber;
    SubRunNumber subrun = InvalidSubRunNumber;
    EventNumber event = InvalidEventNumber;
    std::string label;
    std::string type;
    bool b = unpackInformation(&dataset_id, &run, &subrun, &event, &label, &type);
    if(!b) return "null";
    std::stringstream result;
    result << "{\"dataset_id\": \"" << dataset_id.to_string() << "\"";
    if(run != InvalidRunNumber)
        result << ", \"run\":" << run;
    if(subrun != InvalidSubRunNumber)
        result << ", \"subrun\":" << subrun;
    if(event != InvalidRunNumber)
        result << ", \"event\":" << event;
    if(!label.empty())
        result << ", \"label\": \"" << label << "\"";
    if(!type.empty())
        result << ", \"type\": \"" << type << "\"";
    result << "}";
    return result.str();
}

}
