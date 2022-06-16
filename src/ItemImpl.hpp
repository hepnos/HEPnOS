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
#include "hepnos/ItemType.hpp"
#include "hepnos/ItemDescriptor.hpp"

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

        static size_t descriptorSize(const ItemType& type) {
            size_t s = sizeof(ItemDescriptor);
            if(type == ItemType::EVENT) return s;
            s -= sizeof(EventNumber);
            if(type == ItemType::SUBRUN) return s;
            s -= sizeof(SubRunNumber);
            if(type == ItemType::RUN) return s;
            s -= sizeof(RunNumber);
            return s;
        }
};

}

#endif
