#ifndef __HEPNOS_ITEM_DESCRIPTOR_HPP
#define __HEPNOS_ITEM_DESCRIPTOR_HPP

#include "hepnos/RunNumber.hpp"
#include "hepnos/SubRunNumber.hpp"
#include "hepnos/EventNumber.hpp"
#include "hepnos/UUID.hpp"

namespace hepnos {

struct ItemDescriptor {
    UUID         dataset;
    RunNumber    run;
    SubRunNumber subrun;
    EventNumber  event;

    ItemDescriptor()
    : run(InvalidRunNumber)
    , subrun(InvalidSubRunNumber)
    , event(InvalidEventNumber) {
        std::memset(dataset.data, '\0', sizeof(dataset.data));
    }

    ItemDescriptor(const UUID& ds,
                   const RunNumber& rn = InvalidRunNumber,
                   const SubRunNumber& srn = InvalidSubRunNumber,
                   const EventNumber& evn = InvalidEventNumber)
    : dataset(ds)
    , run(rn)
    , subrun(srn)
    , event(evn) {}

    bool operator==(const ItemDescriptor& other) const {
        return dataset == other.dataset
            && run     == other.run
            && subrun  == other.subrun
            && event   == other.event;
    }

    bool operator!=(const ItemDescriptor& other) const {
        return !(*this == other);
    }

    bool operator<(const ItemDescriptor& other) const {
        // check the level of each ItemDescriptor
        uint8_t level = 2;
        if(event == InvalidEventNumber) {
            if(subrun == InvalidSubRunNumber) level = 0;
            else level = 1;
        }
        uint8_t other_level = 2;
        if(other.event == InvalidEventNumber) {
            if(other.subrun == InvalidSubRunNumber) other_level = 0;
            else other_level = 1;
        }
        if(level < other_level) return true;
        if(level > other_level) return false;
        // here we know the descriptors have the same level, so we compare datasets
        int c = memcmp(dataset.data, other.dataset.data, sizeof(dataset));
        if(c < 0) return true;
        if(c > 0) return false;
        // datasets are the same, we compare run numbers
        if(run < other.run) return true;
        if(run > other.run) return false;
        // runs are the same, we compare subrun numbers
        if(subrun == InvalidSubRunNumber) return false;
        if(subrun < other.subrun) return true;
        if(subrun > other.subrun) return false;
        // subruns are the same, we compare event numbers
        if(event < other.event) return true;
        return false;
    }

    template<typename S>
    friend S& operator<<(S& s, const ItemDescriptor& d);

    std::string to_string() const {
        return std::string("[")
               + dataset.to_string() + ", "
               + std::to_string(run) + ", "
               + std::to_string(subrun) + ", "
               + std::to_string(event) + "]";
    }
};

template<typename S>
S& operator<<(S& s, const ItemDescriptor& d) {
    s << "[" << d.dataset.to_string() << ", "
      << d.run << ", "
      << d.subrun << ", "
      << d.event << "]";
    return s;
}

}

#endif
