/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRODUCT_ID_H
#define __HEPNOS_PRODUCT_ID_H

#include <string>
#include <cstring>
#include <boost/serialization/access.hpp>
#include <boost/serialization/string.hpp>

#include <hepnos/Demangle.hpp>
#include <hepnos/UUID.hpp>
#include <hepnos/RunNumber.hpp>
#include <hepnos/SubRunNumber.hpp>
#include <hepnos/EventNumber.hpp>
#include <hepnos/ItemDescriptor.hpp>

namespace hepnos {

/**
 * @brief Product identifier.
 */
class ProductID {

    friend class DataStore;
    friend class DataStoreImpl;
    friend class AsyncEngineImpl;
    friend class WriteBatchImpl;
    friend class AsyncPrefetcherImpl;
    friend class SyncPrefetcherImpl;
    friend struct ProductCacheImpl;
    friend class ParallelEventProcessorImpl;
    friend class boost::serialization::access;

    public:

    ProductID() = default;

    /**
     * @brief Copy constructor.
     *
     * @param other ProductID to copy.
     */
    ProductID(const ProductID& other) = default;

    /**
     * @brief Move constructor.
     *
     * @param other ProductID to move.
     */
    ProductID(ProductID&& other) = default;

    /**
     * @brief Copy-assignment operator.
     *
     * @param other ProductID to assign.
     *
     * @return Reference to this ProductID.
     */
    ProductID& operator=(const ProductID& other) = default;

    /**
     * @brief Move-assignment operator.
     *
     * @param other ProductID to move from.
     *
     * @return Reference to this ProductID.
     */
    ProductID& operator=(ProductID&& other) = default;

    /**
     * @brief Destructor.
     */
    ~ProductID() = default;

    /**
     * @brief Indicates whether this ProductID instance points to a valid
     * product in the underlying storage service.
     *
     * @return True if the ProductID instance points to a valid product in the
     * underlying service, false otherwise.
     */
    bool valid() const {
        return m_key.size() != 0;
    }

    /**
     * @brief Conversion to bool.
     *
     * @return True if the ProductID instance points to a valid product in the
     * underlying service, false otherwise.
     */
    inline operator bool() const {
        return valid();
    }

    /**
     * @brief Compares this ProductID with another ProductID.
     *
     * @param other ProductID instance to compare against.
     *
     * @return true if the ProductIDs are the same, false otherwise.
     */
    bool operator==(const ProductID& other) const {
        return m_key == other.m_key;
    }

    /**
     * @brief Compares this ProductID with another ProductID.
     *
     * @param other ProductID instance to compare against.
     *
     * @return true if the ProductIDs are different, false otherwise.
     */
    bool operator!=(const ProductID& other) const {
        return !(*this == other);
    }


    /**
     * @brief Unpacks the information contained in the ProductID.
     * All arguments are optional (nullptr may be passed).
     *
     * @param dataset_id
     * @param run
     * @param subrun
     * @param event
     * @param label
     * @param type
     */
    bool unpackInformation(UUID* dataset_id,
                           RunNumber* run,
                           SubRunNumber* subrun,
                           EventNumber* event,
                           std::string* label,
                           std::string* type) const;

    /**
     * @brief Converts the ProductID into a JSON representation.
     *
     * @return a JSON string representing the product.
     */
    std::string toJSON() const;

    template<typename T>
    static ProductID from(const char* label,
                          const UUID& dataset_id,
                          RunNumber run = InvalidRunNumber,
                          SubRunNumber subrun = InvalidSubRunNumber,
                          EventNumber event = InvalidEventNumber);

    private:

    std::string m_key;

    ProductID(const std::string& key)
    : m_key(key) {}

    template<typename Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & m_key;
    }

};

template<typename T>
ProductID ProductID::from(const char* label,
                          const UUID& dataset_id,
                          RunNumber run,
                          SubRunNumber subrun,
                          EventNumber event) {
    ProductID pid;
    size_t label_len = std::strlen(label);
    std::string type_name = demangle<T>();
    ItemDescriptor id(dataset_id, run, subrun, event);
    pid.m_key.resize(label_len + type_name.size() + 1 + sizeof(ItemDescriptor));
    char* s = const_cast<char*>(pid.m_key.c_str());
    std::memcpy(s, &id, sizeof(id));
    s += sizeof(id);
    std::memcpy(s, label, label_len);
    s += label_len;
    s[0] = '#';
    s += 1;
    std::memcpy(s, type_name.c_str(), type_name.size());
    return pid;
}

}

#endif
