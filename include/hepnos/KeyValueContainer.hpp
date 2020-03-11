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
#include <hepnos/DataStore.hpp>

namespace hepnos {

class WriteBatch;
class AsyncEngine;
class Prefetcher;

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
    virtual DataStore datastore() const = 0;

    /**
     * @brief Stores raw key/value data in this KeyValueContainer.
     * This function is virtual and must be overloaded in the child class.
     *
     * @param key Key
     * @param value Value pointer
     * @param vsize Value size (in bytes)
     *
     * @return A valid ProductID if the key did not already exist, an invalid one otherwise.
     */
    virtual ProductID storeRawData(const std::string& key, const char* value, size_t vsize) = 0;

    /**
     * @brief Stores raw key/value data in a WriteBatch.
     * This function is virtual and must be overloaded in the child class.
     *
     * @param batch Batch in which to write.
     * @param key Key
     * @param value Value pointer
     * @param vsize Value size (in bytes)
     *
     * @return 
     */
    virtual ProductID storeRawData(WriteBatch& batch, const std::string& key, const char* value, size_t vsize) = 0;

    virtual ProductID storeRawData(AsyncEngine& async, const std::string& key, const char* value, size_t vsize) = 0;

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

    virtual bool loadRawData(const std::string& key, char* value, size_t* vsize) const = 0;

    virtual bool loadRawData(const Prefetcher& prefetcher, const std::string& key, std::string& buffer) const = 0;

    virtual bool loadRawData(const Prefetcher& prefetcher, const std::string& key, char* value, size_t* vsize) const = 0;

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
        return storeImpl(key, value, std::is_pod<V>());
    }

    /**
     * @brief Stores a key/value pair into the WriteBatch.
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
    ProductID store(WriteBatch& batch, const K& key, const V& value) {
        return storeImpl(batch, key, value, std::is_pod<V>());
    }

    template<typename K, typename V>
    ProductID store(AsyncEngine& async, const K& key, const V& value) {
        return storeImpl(async, key, value, std::is_pod<V>());
    }

    /**
     * @brief Version of store when the value is an std::vector.
     */
    template<typename K, typename V>
    ProductID store(const K& key, const std::vector<V>& value, int start=0, int end=-1) {
        std::string key_str, val_str;
        serializeKeyValueVector(std::is_pod<V>(), key, value, key_str, val_str, start, end);
        return storeRawData(key_str, val_str.data(), val_str.size());
    }

    /**
     * @brief Version of store when the value is an std::vector.
     */
    template<typename K, typename V>
    ProductID store(WriteBatch& batch, const K& key, const std::vector<V>& value, int start=0, int end=-1) {
        std::string key_str, val_str;
        serializeKeyValueVector(std::is_pod<V>(), key, value, key_str, val_str, start, end);
        return storeRawData(batch, key_str, val_str.data(), val_str.size());
    }

    /**
     * @brief Version of store when the value is an std::vector.
     */
    template<typename K, typename V>
    ProductID store(AsyncEngine& async, const K& key, const std::vector<V>& value, int start=0, int end=-1) {
        std::string key_str, val_str;
        serializeKeyValueVector(std::is_pod<V>(), key, value, key_str, val_str, start, end);
        return storeRawData(async, key_str, val_str.data(), val_str.size());
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
        return loadImpl(key, value, std::is_pod<V>());
    }

    /**
     * @brief Version of load for vectors.
     */
    template<typename K, typename V>
    bool load(const K& key, std::vector<V>& value) const {
        return loadVectorImpl(key, value, std::is_pod<V>());
    }

    template<typename K, typename V>
    bool load(const Prefetcher& prefetcher, const K& key, V& value) const {
        return loadImpl(prefetcher, key, value, std::is_pod<V>());
    }

    template<typename K, typename V>
    bool load(const Prefetcher& prefetcher, const K& key, std::vector<V>& value) const {
        return loadVectorImpl(prefetcher, key, value, std::is_pod<V>());
    }

    private:

    /**
     * @brief Implementation of the store function when the value is
     * not an std::vector and not a POD.
     */
    template<typename K, typename V>
    ProductID storeImpl(const K& key, const V& value,
            const std::integral_constant<bool, false>&) {
        std::string key_str, val_str;
        serializeKeyValue(key, value, key_str, val_str);
        return storeRawData(key_str, val_str.data(), val_str.size());
    }

    /**
     * @brief Implementation of the store function with WriteBatch
     * and the value type is not am std::vector and not a POD.
     */
    template<typename K, typename V>
    ProductID storeImpl(WriteBatch& batch, const K& key, const V& value,
            const std::integral_constant<bool, false>&) {
        std::string key_str, val_str;
        serializeKeyValue(key, value, key_str, val_str);
        return storeRawData(batch, key_str, val_str.data(), val_str.size());
    }

    /**
     * @brief Implementation of the store function with AsyncEngine
     * and the value type is not am std::vector and not a POD.
     */
    template<typename K, typename V>
    ProductID storeImpl(AsyncEngine& async, const K& key, const V& value,
            const std::integral_constant<bool, false>&) {
        std::string key_str, val_str;
        serializeKeyValue(key, value, key_str, val_str);
        return storeRawData(async, key_str, val_str.data(), val_str.size());
    }

    /**
     * @brief Implementation of the store function when the value
     * type is a POD.
     */
    template<typename K, typename V>
    ProductID storeImpl(const K& key, const V& value,
            const std::integral_constant<bool, true>&) {
        std::string key_str;
        serializeKeyValue(key, value, key_str);
        return storeRawData(key_str, reinterpret_cast<const char*>(&value), sizeof(value));
    }

    /**
     * @brief Implementation of the store function with WriteBatch
     * when the value type is a POD.
     */
    template<typename K, typename V>
    ProductID storeImpl(WriteBatch& batch, const K& key, const V& value,
            const std::integral_constant<bool, true>&) {
        std::string key_str;
        serializeKeyValue(key, value, key_str);
        return storeRawData(batch, key_str, reinterpret_cast<const char*>(&value), sizeof(value));
    }

    /**
     * @brief Implementation of the store function with AsyncEngine
     * when the value type is a POD.
     */
    template<typename K, typename V>
    ProductID storeImpl(AsyncEngine& async, const K& key, const V& value,
            const std::integral_constant<bool, true>&) {
        std::string key_str;
        serializeKeyValue(key, value, key_str);
        return storeRawData(async, key_str, reinterpret_cast<const char*>(&value), sizeof(value));
    }

    /**
     * @brief Implementation of the load function when the value type is a POD.
     */
    template<typename K, typename V>
    bool loadImpl(const K& key, V& value,
            const std::integral_constant<bool, true>&) const {
        std::string buffer;
        std::stringstream ss_key;
        ss_key << key << "#" << demangle<V>();
        size_t vsize = sizeof(value);
        if(!loadRawData(ss_key.str(), reinterpret_cast<char*>(&value), &vsize)) {
            return false;
        }
        return vsize == sizeof(value);
    }

    template<typename K, typename V>
    bool loadImpl(const Prefetcher& prefetcher, const K& key, V& value,
            const std::integral_constant<bool, true>&) const {
        std::string buffer;
        std::stringstream ss_key;
        ss_key << key << "#" << demangle<V>();
        size_t vsize = sizeof(value);
        if(!loadRawData(prefetcher, ss_key.str(), reinterpret_cast<char*>(&value), &vsize)) {
            return false;
        }
        return vsize == sizeof(value);
    }

    /**
     * @brief Implementation of the load function when the value type is not a POD.
     */
    template<typename K, typename V>
    bool loadImpl(const K& key, V& value,
            const std::integral_constant<bool, false>&) const {
        std::string buffer;
        std::stringstream ss_key;
        ss_key << key << "#" << demangle<V>();
        if(!loadRawData(ss_key.str(), buffer)) {
            return false;
        }
        try {
            std::stringstream ss(buffer);
            InputArchive ia(datastore(), ss);
            ia >> value;
        } catch(...) {
            throw Exception("Exception occured during serialization");
        }
        return true;
    }

    template<typename K, typename V>
    bool loadImpl(const Prefetcher& prefetcher, const K& key, V& value,
            const std::integral_constant<bool, false>&) const {
        std::string buffer;
        std::stringstream ss_key;
        ss_key << key << "#" << demangle<V>();
        if(!loadRawData(prefetcher, ss_key.str(), buffer)) {
            return false;
        }
        try {
            std::stringstream ss(buffer);
            InputArchive ia(datastore(), ss);
            ia >> value;
        } catch(...) {
            throw Exception("Exception occured during serialization");
        }
        return true;
    }

    /**
     * @brief Implementation of the load function when the value type is a vector of POD.
     */
    template<typename K, typename V>
    bool loadVectorImpl(const K& key, std::vector<V>& value,
            const std::integral_constant<bool, true>&) const {
        std::string buffer;
        std::stringstream ss_key;
        ss_key << key << "#" << demangle<std::vector<V>>();
        if(!loadRawData(ss_key.str(), buffer)) {
            return false;
        }
        size_t count = 0;
        if(buffer.size() < sizeof(count)) {
            return false;
        }
        std::memcpy(&count, buffer.data(), sizeof(count));
        if(buffer.size() != sizeof(count) + count*sizeof(V)) {
            return false;
        }
        value.resize(count);
        std::memcpy(value.data(), buffer.data()+sizeof(count), count*sizeof(V));
        return true;
    }

    template<typename K, typename V>
    bool loadVectorImpl(const Prefetcher& prefetcher, const K& key, std::vector<V>& value,
            const std::integral_constant<bool, true>&) const {
        std::string buffer;
        std::stringstream ss_key;
        ss_key << key << "#" << demangle<std::vector<V>>();
        if(!loadRawData(prefetcher, ss_key.str(), buffer)) {
            return false;
        }
        size_t count = 0;
        if(buffer.size() < sizeof(count)) {
            return false;
        }
        std::memcpy(&count, buffer.data(), sizeof(count));
        if(buffer.size() != sizeof(count) + count*sizeof(V)) {
            return false;
        }
        value.resize(count);
        std::memcpy(value.data(), buffer.data()+sizeof(count), count*sizeof(V));
        return true;
    }

    /**
     * @brief Implementation of the load function when the value type is a vector of non-POD.
     */
    template<typename K, typename V>
    bool loadVectorImpl(const K& key, std::vector<V>& value,
            const std::integral_constant<bool, false>&) const {
        std::string buffer;
        std::stringstream ss_key;
        ss_key << key << "#" << demangle<std::vector<V>>();
        if(!loadRawData(ss_key.str(), buffer)) {
            return false;
        }
        try {
            std::stringstream ss(buffer);
            InputArchive ia(datastore(), ss);
            size_t count = 0;
            ia >> count;
            value.resize(count);
            for(unsigned i=0; i<count; i++) {
                ia >> value[i];
            }
        } catch(...) {
            throw Exception("Exception occured during serialization");
        }
        return true;
    }

    template<typename K, typename V>
    bool loadVectorImpl(const Prefetcher& prefetcher, const K& key, std::vector<V>& value,
            const std::integral_constant<bool, false>&) const {
        std::string buffer;
        std::stringstream ss_key;
        ss_key << key << "#" << demangle<std::vector<V>>();
        if(!loadRawData(prefetcher, ss_key.str(), buffer)) {
            return false;
        }
        try {
            std::stringstream ss(buffer);
            InputArchive ia(datastore(), ss);
            size_t count = 0;
            ia >> count;
            value.resize(count);
            for(unsigned i=0; i<count; i++) {
                ia >> value[i];
            }
        } catch(...) {
            throw Exception("Exception occured during serialization");
        }
        return true;
    }

    /**
     * @brief Creates the string key based on the provided key
     * and the type of the value. Serializes the value into a string.
     */
    template<typename K, typename V>
    static void serializeKeyValue(const K& key, const V& value,
            std::string& key_str, std::string& value_str) {
        serializeKeyValue(key, value, key_str);
        std::stringstream ss_value;
        boost::archive::binary_oarchive oa(ss_value, boost::archive::archive_flags::no_header);
        try {
            oa << value;
        } catch(...) {
            throw Exception("Exception occured during serialization");
        }
        value_str = ss_value.str();
    }

    /**
     * @brief Creates the string key based on the provided key
     * and the type of the value.
     */
    template<typename K, typename V>
    static void serializeKeyValue(const K& key, const V& value, std::string& key_str) {
        std::stringstream ss_key;
        ss_key << key << "#" << demangle<V>();
        key_str = std::move(ss_key.str());
    }

    /**
     * @brief Version of serializeKeyValue for vectors of non-POD datatypes.
     */
    template<typename K, typename V>
    static void serializeKeyValueVector(const std::integral_constant<bool, false>&,
            const K& key, const std::vector<V>& value,
            std::string& key_str, std::string& value_str,
            int start, int end) {
        if(end == -1)
            end = value.size();
        if(start < 0 || start > end || end > value.size())
            throw Exception("Invalid range when storing vector");
        serializeKeyValue(key, value, key_str);
        std::stringstream ss_value;
        boost::archive::binary_oarchive oa(ss_value, boost::archive::archive_flags::no_header);
        try {
            size_t count = end-start;
            oa << count;
            for(auto i = start; i < end; i++)
                oa << value[i];
        } catch(...) {
            throw Exception("Exception occured during serialization");
        }
        value_str = ss_value.str();
    }

    /**
     * @brief Version of serializeKeyValue for vectors of POD datatypes.
     */
    template<typename K, typename V>
    static void serializeKeyValueVector(const std::integral_constant<bool, true>&,
            const K& key, const std::vector<V>& value,
            std::string& key_str, std::string& value_str,
            int start, int end) {
        if(end == -1)
            end = value.size();
        if(start < 0 || start > end || end > value.size())
            throw Exception("Invalid range when storing vector");
        serializeKeyValue(key, value, key_str);
        size_t count = end-start;
        value_str.resize(sizeof(count) + count*sizeof(V));
        std::memcpy(const_cast<char*>(value_str.data()), &count, sizeof(count));
        std::memcpy(const_cast<char*>(value_str.data())+sizeof(count), &value[start], count*sizeof(V));
    }
};

}

#endif
