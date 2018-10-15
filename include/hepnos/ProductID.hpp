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

#include <hepnos/DataStore.hpp>

namespace hepnos {

class ProductID {

    friend class DataStore;
    friend class DataStore::Impl;
    friend class boost::serialization::access;

    private:

    std::uint8_t m_level;
    std::string  m_containerName;
    std::string  m_objectName;

    ProductID(std::uint8_t level, const std::string& containerName, const std::string& objectName)
    : m_level(level)
    , m_containerName(containerName)
    , m_objectName(objectName) {}

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
        return m_objectName.size() != 0;
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
        return m_level == other.m_level
            && m_containerName == other.m_containerName
            && m_objectName == other.m_objectName;
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

    template<typename Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & m_level;
        ar & m_containerName;
        ar & m_objectName;
    }

};

}

#endif
