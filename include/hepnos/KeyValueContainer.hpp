/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_KEYVAL_CONTAINER_H
#define __HEPNOS_KEYVAL_CONTAINER_H

#include <memory>
#include <string>
#include <sstream>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/serialization/string.hpp>
#include <hepnos/InputArchive.hpp>
#include <hepnos/ProductID.hpp>
#include <hepnos/Demangle.hpp>
#include <hepnos/Exception.hpp>

namespace hepnos {

class WriteBatch;

class KeyValueContainer {

    public:

    /**
     * @brief Default constructor.
     */
    KeyValueContainer() = default;

    /**
     * @brief Copy constructor.
     */
    KeyValueContainer(const KeyValueContainer& other) = default;

    /**
     * @brief Move constructor.
     */
    KeyValueContainer(KeyValueContainer&& other) = default;

    /**
     * @brief Copy-assignment operator.
     */
    KeyValueContainer& operator=(const KeyValueContainer& other) = default;

    /**
     * @brief Move-assignment operator.
     */
    KeyValueContainer& operator=(KeyValueContainer&& other) = default;

    /**
     * @brief Destructor.
     */
    virtual ~KeyValueContainer() = default;

    /**
     * @brief Gets the DataStore to which this instance of KeyValueContainer belongs.
     *
     * @return DataStore.
     */
    virtual DataStore* getDataStore() const = 0;

    /**
     * @brief Stores raw key/value data in this KeyValueContainer.
     * This function is virtual and must be overloaded in the child class.
     *
     * @param key Key
     * @param buffer Value
     *
     * @return A valid ProductID if the key did not already exist, an invalid one otherwise.
     */
    virtual ProductID storeRawData(const std::string& key, const std::string& value) = 0;

    virtual ProductID storeRawData(std::string&& key, std::string&& value) = 0;

    virtual ProductID storeRawData(WriteBatch& batch, const std::string& key, const std::string& value) = 0;

    virtual ProductID storeRawData(WriteBatch& batch, std::string&& key, std::string&& value) = 0;

    /**
     * @brief Loads raw key/value data from this KeyValueContainer.
     * This function is virtual and must be overloaded in the child class.
     *
     * @param key Key
     * @param buffer Buffer used to hold the value.
     *
     * @return true if the key exists, false otherwise.
     */
    virtual bool loadRawData(const std::string& key, std::string& buffer) const = 0;

    /**
     * @brief Stores a key/value pair into the KeyValueContainer.
     * The type of the key should have operator<< available
     * to stream it into a std::stringstream for the purpose
     * of converting it into an std::string. The resulting
     * string must not have the "/", or "%" characters. The
     * type of the value must be serializable using Boost.
     *
     * @tparam K type of the key.
     * @tparam V type of the value.
     * @param key Key to store.
     * @param value Value to store.
     *
     * @return true if the key was found. false otherwise.
     */
    template<typename K, typename V>
    ProductID store(const K& key, const V& value) {
        std::string key_str, val_str;
        serializeKeyValue(key, value, key_str, val_str);
        return storeRawData(std::move(key_str), std::move(val_str));
    }

    template<typename K, typename V>
    ProductID store(WriteBatch& batch, const K& key, const V& value) {
        std::string key_str, val_str;
        serializeKeyValue(key, value, key_str, val_str);
        return storeRawData(batch, std::move(key_str), std::move(val_str));
    }

    /**
     * @brief Loads a value associated with a key from the 
     * KeyValueContainer. The type of the key should have 
     * operator<< available to stream it into a std::stringstream 
     * for the purpose of converting it into an std::string. 
     * The resulting string must not have the "/" or "%" characters.
     * The type of the value must be serializable using Boost.
     *
     * @tparam K type of the key.
     * @tparam V type of the value.
     * @param key Key to load.
     * @param value Value to load.
     *
     * @return true if the key exists and was loaded. False otherwise.
     */
    template<typename K, typename V>
    bool load(const K& key, V& value) const {
        std::string buffer;
        std::stringstream ss_key;
        ss_key << key << "#" << demangle<V>();
        if(!loadRawData(ss_key.str(), buffer)) {
            return false;
        }
        try {
            std::stringstream ss(buffer);
            //boost::archive::binary_iarchive ia(ss);
            InputArchive ia(getDataStore(), ss);
            ia >> value;
        } catch(...) {
            throw Exception("Exception occured during serialization");
        }
        return true;
    }

    private:

    template<typename K, typename V>
    static void serializeKeyValue(const K& key, const V& value,
            std::string& key_str, std::string& value_str) {
        std::stringstream ss_value;
        std::stringstream ss_key;
        ss_key << key << "#" << demangle<V>();
        key_str = std::move(ss_key.str());
        boost::archive::binary_oarchive oa(ss_value);
        try {
            oa << value;
        } catch(...) {
            throw Exception("Exception occured during serialization");
        }
        value_str = ss_value.str();
    }
};

}

#endif
