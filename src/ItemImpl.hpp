/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRIVATE_ITEM_IMPL_H
#define __HEPNOS_PRIVATE_ITEM_IMPL_H

#include <sstream>
#include <iomanip>
#include <memory>
#include "hepnos/Run.hpp"
#include "hepnos/UUID.hpp"
#include "ItemDescriptor.hpp"

namespace hepnos {

class DataStoreImpl;

class ItemImpl {

    public:

        std::shared_ptr<DataStoreImpl> m_datastore;
        ItemDescriptor                 m_descriptor;

        ItemImpl(const std::shared_ptr<DataStoreImpl>& ds,
                 const UUID& dataset,
                 const RunNumber& rn,
                 const SubRunNumber& srn = InvalidSubRunNumber,
                 const EventNumber& evn = InvalidEventNumber)
        : m_datastore(ds)
        , m_descriptor(dataset, rn, srn, evn) {}

        ItemImpl(const std::shared_ptr<DataStoreImpl>& ds,
                 const ItemDescriptor& descriptor)
        : m_datastore(ds)
        , m_descriptor(descriptor) {}

        bool operator==(const ItemImpl& other) const {
            return m_descriptor == other.m_descriptor;
        }

        size_t parentPrefixSize() const {
            size_t s = sizeof(ItemDescriptor) - sizeof(m_descriptor.event);
            if(m_descriptor.event == InvalidEventNumber) 
                s -= sizeof(m_descriptor.subrun);
            if(m_descriptor.subrun == InvalidSubRunNumber)
                s -= sizeof(m_descriptor.run);
            return s;
        }
};

}

#endif
