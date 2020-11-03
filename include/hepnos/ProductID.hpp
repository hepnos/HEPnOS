/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRODUCT_ID_H
#define __HEPNOS_PRODUCT_ID_H

#include <string>

#include <boost/serialization/access.hpp>
#include <boost/serialization/string.hpp>


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

    private:

    std::string m_key;

    ProductID(const std::string& key)
    : m_key(key) {}

    template<typename Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & m_key;
    }

};

}

#endif
