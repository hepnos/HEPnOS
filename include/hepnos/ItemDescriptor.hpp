/*
 * (C) 2022 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_ITEM_DESCRIPTOR_HPP
#define __HEPNOS_ITEM_DESCRIPTOR_HPP

#include <boost/serialization/binary_object.hpp>
#include <hepnos/RunNumber.hpp>
#include <hepnos/SubRunNumber.hpp>
#include <hepnos/EventNumber.hpp>
#include <hepnos/UUID.hpp>
#include <hepnos/BigEndian.hpp>

namespace hepnos {

constexpr const int EventDescriptorLength  = 40;
constexpr const int SubRunDescriptorLength = 32;
constexpr const int RunDescriptorLength    = 24;

struct EventDescriptor {
    char data[EventDescriptorLength];
};

struct SubRunDescriptor {
    char data[SubRunDescriptorLength];

    template<typename Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & boost::serialization::make_binary_object(
                static_cast<void*>(this), sizeof(*this));
    }
};

struct RunDescriptor {
    char data[RunDescriptorLength];

    template<typename Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & boost::serialization::make_binary_object(
                static_cast<void*>(this), sizeof(*this));
    }
};

struct ItemDescriptor {
    UUID             dataset;
    BE<RunNumber>    run;
    BE<SubRunNumber> subrun;
    BE<EventNumber>  event;

    ItemDescriptor()
    : run(InvalidRunNumber)
    , subrun(InvalidSubRunNumber)
    , event(InvalidEventNumber) {
        std::memset(dataset.data, '\0', sizeof(dataset.data));
    }

    ItemDescriptor(const EventDescriptor& ed) {
        std::memcpy(this, &ed, EventDescriptorLength);
    }

    ItemDescriptor(const SubRunDescriptor& srd) {
        std::memcpy(this, &srd, SubRunDescriptorLength);
        event = InvalidEventNumber;
    }

    ItemDescriptor(const RunDescriptor& rd) {
        std::memcpy(this, &rd, RunDescriptorLength);
        event = InvalidEventNumber;
        subrun = InvalidSubRunNumber;
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
    s << "[" << d.dataset.to_string();
    if(d.run != InvalidRunNumber) {
        s << ", " << d.run;
        if(d.subrun != InvalidSubRunNumber) {
            s << ", " << d.subrun;
            if(d.event != InvalidEventNumber) {
                s << ", " << d.event;
            }
        }
    }
    s << "]";
    return s;
}

}

#endif
