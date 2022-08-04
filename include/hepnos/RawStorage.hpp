/*
 * (C) 2022 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_RAW_STORAGE_H
#define __HEPNOS_RAW_STORAGE_H

#include <memory>
#include <string>
#include <sstream>
#include <boost/serialization/string.hpp>
#include <hepnos/Statistics.hpp>
#include <hepnos/ProductID.hpp>
#include <hepnos/Exception.hpp>

namespace hepnos {

class KeyValueContainer;

class RawStorage {

    friend class KeyValueContainer;

    public:

    /**
     * @brief Default constructor.
     */
    RawStorage() = default;

    /**
     * @brief Copy constructor.
     */
    RawStorage(const RawStorage& other) = default;

    /**
     * @brief Move constructor.
     */
    RawStorage(RawStorage&& other) = default;

    /**
     * @brief Copy-assignment operator.
     */
    RawStorage& operator=(const RawStorage& other) = default;

    /**
     * @brief Move-assignment operator.
     */
    RawStorage& operator=(RawStorage&& other) = default;

    /**
     * @brief Destructor.
     */
    virtual ~RawStorage() = default;

    /**
     * @brief Check if the object is valid.
     */
    virtual bool valid() const = 0;

    protected:

    /**
     * @brief Stores raw key/value data in this RawStorage.
     * This function is virtual and must be overloaded in the child class.
     *
     * @param key Key
     * @param value Value pointer
     * @param vsize Value size (in bytes)
     *
     * @return A valid ProductID if the key did not already exist, an invalid one otherwise.
     */
    virtual ProductID storeRawData(const ProductID& key, const char* value, size_t vsize) = 0;

    /**
     * @brief Loads raw key/value data from this RawStorage.
     * This function is virtual and must be overloaded in the child class.
     *
     * @param key Key
     * @param buffer Buffer used to hold the value.
     *
     * @return true if the key exists, false otherwise.
     */
    virtual bool loadRawData(const ProductID& key, std::string& buffer) const = 0;

    /**
     * @brief Loads binary data associated with a particular key from the container.
     * This function will return true if the key exists and the read succeeded.
     * It will return false otherwise.
     *
     * @param key Key.
     * @param value Buffer in which to put the binary data.
     * @param vsize in: size of the buffer, out: actual size of the data.
     *
     * @return true if the key exists and the read succeeded, false otherwise.
     */
    virtual bool loadRawData(const ProductID& key, char* value, size_t* vsize) const = 0;

};

}

#endif
