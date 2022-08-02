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
#include <boost/serialization/string.hpp>
#include <hepnos/Statistics.hpp>
#include <hepnos/OutputArchive.hpp>
#include <hepnos/InputArchive.hpp>
#include <hepnos/ProductID.hpp>
#include <hepnos/Demangle.hpp>
#include <hepnos/Exception.hpp>
#include <hepnos/DataStore.hpp>

namespace hepnos {

class WriteBatch;
class AsyncEngine;
class Prefetcher;
class ProductCache;

struct StoreStatistics {
    Statistics<double> raw_storage_time;
    Statistics<double> serialization_time;
};

struct LoadStatistics {
    Statistics<double> raw_loading_time;
    Statistics<double> deserialization_time;
};

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
     * Note since the WriteBatch is flushed later to the DataStore, the DataStore will
     * not be able to check whether the product could be created or not. Hence the
     * ProductID returned is valid but may not ultimately correspond to an actual
     * Product in the DataStore, should the storage operation fail.
     *
     * @param batch Batch in which to write.
     * @param key Key
     * @param value Value pointer
     * @param vsize Value size (in bytes)
     *
     * @return A valid ProductID.
     */
    virtual ProductID storeRawData(WriteBatch& batch, const std::string& key, const char* value, size_t vsize) = 0;

    /**
     * @brief Stores binary data associated with a particular key using the AsyncEngine.
     * Note since the AsyncEngine makes the operation happen later in the background,
     * the DataStore will not be able to check whether the product could be created or not.
     * Hence the ProductID returned is valid but may not ultimately correspond to an actual
     * Product in the DataStore, should the storage operation fail.
     *
     * @param engine AsyncEngine to use to write asynchronously.
     * @param key Key.
     * @param value Binary data to write.
     * @param vsize Size of the data (in bytes).
     *
     * @return a valid ProductID.
     */
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
    virtual bool loadRawData(const std::string& key, char* value, size_t* vsize) const = 0;

    /**
     * @brief Loads binary data associated with a particular key from the container.
     * This function will look in the prefetcher if the object has been prefetched
     * (or scheduled to be prefetched) and fall back to looking up in the underlying
     * DataStore if it hasn't.
     *
     * @param prefetcher Prefetcher to look into first.
     * @param key Key.
     * @param buffer Buffer in which to put the binary data.
     *
     * @return true if the key exists and the read succeeded, false otherwise.
     */
    virtual bool loadRawData(const Prefetcher& prefetcher, const std::string& key, std::string& buffer) const = 0;

    /**
     * @brief Loads binary data associated with a particular key from the container.
     * This function will look in the prefetcher if the object has been prefetched
     * (or scheduled to be prefetched) and fall back to looking up in the underlying
     * DataStore if it hasn't.
     *
     * @param prefetcher Prefetcher to look into first.
     * @param key Key.
     * @param value Buffer in which to put the binary data.
     * @param vsize in: size of the buffer, out: size of the actual data.
     *
     * @return true if the key exists and the read succeeded, false otherwise.
     */
    virtual bool loadRawData(const Prefetcher& prefetcher, const std::string& key, char* value, size_t* vsize) const = 0;

    /**
     * @brief Loads binary data associated with a particular key from the container.
     * This function will look in the product cache for the requested object.
     * Note that contrary to the Prefetcher, this function will NOT fall back to looking
     * up into the DataStore.
     *
     * @param cache ProductCache to look into first.
     * @param key Key.
     * @param buffer Buffer in which to put the binary data.
     *
     * @return true if the key exists and the read succeeded, false otherwise.
     */
    virtual bool loadRawData(const ProductCache& cache, const std::string& key, std::string& buffer) const = 0;

    /**
     * @brief Loads binary data associated with a particular key from the container.
     * This function will look in the product cache for the requested object.
     * Note that contrary to the Prefetcher, this function will NOT fall back to looking
     * up into the DataStore.
     *
     * @param cache ProductCache to look into first.
     * @param key Key.
     * @param value Buffer in which to put the binary data.
     * @param vsize in: size of the buffer, out: size of the actual data.
     *
     * @return true if the key exists and the read succeeded, false otherwise.
     */
    virtual bool loadRawData(const ProductCache& cache, const std::string& key, char* value, size_t* vsize) const = 0;

    /**
     * @brief Stores a key/value pair into the KeyValueContainer.
     * The type of the key should have operator<< available
     * to stream it into a std::stringstream for the purpose
     * of converting it into an std::string. The resulting
     * string must not have the "/", or "%" characters. The
     * type of the value must be serializable using Boost.
     *
     * @tparam L type of the label.
     * @tparam V type of the value.
     * @param label Label to store.
     * @param value Value to store.
     * @param stats Statistics.
     *
     * @return a valid ProductID if the key was found, an invalid one otherwise.
     */
    template<typename L, typename V>
    ProductID store(const L& label, const V& value, StoreStatistics* stats = nullptr) {
        return storeImpl(label, value, std::is_pod<std::remove_reference_t<V>>(), stats);
    }

    /**
     * @brief Stores a key/value pair into the WriteBatch.
     * The type of the key should have operator<< available
     * to stream it into a std::stringstream for the purpose
     * of converting it into an std::string. The resulting
     * string must not have the "/", or "%" characters. The
     * type of the value must be serializable using Boost.
     *
     * Note that since the WriteBatch will delay the operation,
     * the returned ProductID is valid even though the operation
     * may not ultimately succeed.
     *
     * @tparam L type of the label.
     * @tparam V type of the value.
     * @param label Label to store.
     * @param value Value to store.
     *
     * @return a valid ProductID.
     */
    template<typename L, typename V>
    ProductID store(WriteBatch& batch, const L& label, const V& value,
                    StoreStatistics* stats = nullptr) {
        return storeImpl(batch, label, value, std::is_pod<std::remove_reference_t<V>>(), stats);
    }

    /**
     * @brief Stores a key/value pair into the WriteBatch.
     * The type of the key should have operator<< available
     * to stream it into a std::stringstream for the purpose
     * of converting it into an std::string. The resulting
     * string must not have the "/", or "%" characters. The
     * type of the value must be serializable using Boost.
     *
     * Note that since the WriteBatch will delay the operation,
     * the returned ProductID is valid even though the operation
     * may not ultimately succeed.
     *
     * @tparam L type of the label.
     * @tparam V type of the value.
     * @param label Label to store.
     * @param value Value to store.
     *
     * @return a valid ProductID.
     */
    template<typename L, typename V>
    ProductID store(AsyncEngine& async, const L& label, const V& value, StoreStatistics* stats = nullptr) {
        return storeImpl(async, label, value, std::is_pod<std::remove_reference_t<V>>(), stats);
    }

    /**
     * @brief Version of store when the value is an std::vector.
     */
    template<typename L, typename V>
    ProductID store(const L& label, const std::vector<V>& value, int start=0, int end=-1,
                    StoreStatistics* stats = nullptr) {
        auto t1 = wtime();
        auto key_str = makeKey(label, value);
        std::string val_str;
        serializeValueVector(std::is_pod<std::remove_reference_t<V>>(), value, val_str, start, end);
        auto t2 = wtime();
        auto result = storeRawData(key_str, val_str.data(), val_str.size());
        auto t3 = wtime();
        if(stats) {
            stats->serialization_time.updateWith(t2-t1);
            stats->raw_storage_time.updateWith(t3-t2);
        }
        return result;
    }

    /**
     * @brief Version of store when the value is an std::vector.
     */
    template<typename L, typename V>
    ProductID store(WriteBatch& batch, const L& label, const std::vector<V>& value, int start=0, int end=-1,
                    StoreStatistics* stats = nullptr) {
        auto t1 = wtime();
        auto key_str = makeKey(label, value);
        std::string val_str;
        serializeValueVector(std::is_pod<std::remove_reference_t<V>>(), value, val_str, start, end);
        auto t2 = wtime();
        auto result = storeRawData(batch, key_str, val_str.data(), val_str.size());
        auto t3 = wtime();
        if(stats) {
            stats->serialization_time.updateWith(t2-t1);
            stats->raw_storage_time(t3-t2);
        }
        return result;
    }

    /**
     * @brief Version of store when the value is an std::vector.
     */
    template<typename L, typename V>
    ProductID store(AsyncEngine& async, const L& label, const std::vector<V>& value, int start=0, int end=-1,
                    StoreStatistics* stats = nullptr) {
        auto t1 = wtime();
        auto key_str = makeKey(label, value);
        std::string val_str;
        serializeValueVector(std::is_pod<std::remove_reference_t<V>>(), value, val_str, start, end);
        auto t2 = wtime();
        auto result = storeRawData(async, key_str, val_str.data(), val_str.size());
        auto t3 = wtime();
        if(stats) {
            stats->serialization_time.updateWith(t2-t1);
            stats->raw_storage_time(t3-t2);
        }
        return result;
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
    template<typename L, typename V>
    bool load(const L& label, V& value, LoadStatistics* stats = nullptr) const {
        return loadImpl(label, value, std::is_pod<std::remove_reference_t<V>>(), stats);
    }

    /**
     * @brief Version of load for vectors.
     */
    template<typename L, typename V>
    bool load(const L& label, std::vector<V>& value, LoadStatistics* stats = nullptr) const {
        return loadVectorImpl(label, value, std::is_pod<std::remove_reference_t<V>>(), stats);
    }

    /**
     * @brief Version of load that will first look into the Prefetcher
     * argument for the requested key.
     */
    template<typename L, typename V>
    bool load(const Prefetcher& prefetcher, const L& label, V& value,
              LoadStatistics* stats = nullptr) const {
        return loadImpl(prefetcher, label, value, std::is_pod<std::remove_reference_t<V>>(), stats);
    }

    /**
     * @brief Version of load for vectors, looking first into the
     * Prefetcher argument for the requested key.
     */
    template<typename L, typename V>
    bool load(const Prefetcher& prefetcher, const L& label, std::vector<V>& value,
              LoadStatistics* stats = nullptr) const {
        return loadVectorImpl(prefetcher, label, value,
                std::is_pod<std::remove_reference_t<V>>(), stats);
    }

    /**
     * @brief Version of load that will first look into the Prefetcher
     * argument for the requested key.
     */
    template<typename L, typename V>
    bool load(const ProductCache& cache, const L& label, V& value, LoadStatistics* stats = nullptr) const {
        return loadImpl(cache, label, value, std::is_pod<std::remove_reference_t<V>>(), stats);
    }

    /**
     * @brief Version of load for vectors, looking first into the
     * Prefetcher argument for the requested key.
     */
    template<typename L, typename V>
    bool load(const ProductCache& cache, const L& label, std::vector<V>& value,
              LoadStatistics* stats = nullptr) const {
        return loadVectorImpl(cache, label, value, std::is_pod<std::remove_reference_t<V>>(), stats);
    }

    /**
     * @brief List all the product ids contained in this container.
     *
     * @param label Optional label to filter products.
     *
     * @return List of product ids.
     */
    virtual std::vector<ProductID> listProducts(const std::string& label="") const = 0;

    private:

    /**
     * @brief Implementation of the store function when the value is
     * not an std::vector and not a POD.
     */
    template<typename L, typename V>
    ProductID storeImpl(const L& label, const V& value,
            const std::integral_constant<bool, false>&, StoreStatistics* stats) {
        auto t1 = wtime();
        auto key_str = makeKey(label, value);
        std::string val_str;
        serializeValue(value, val_str);
        auto t2 = wtime();
        auto result = storeRawData(key_str, val_str.data(), val_str.size());
        auto t3 = wtime();
        if(stats) {
            stats->serialization_time.updateWith(t2-t1);
            stats->raw_storage_time.updateWith(t3-t2);
        }
        return result;
    }

    /**
     * @brief Implementation of the store function with WriteBatch
     * and the value type is not am std::vector and not a POD.
     */
    template<typename L, typename V>
    ProductID storeImpl(WriteBatch& batch, const L& label, const V& value,
            const std::integral_constant<bool, false>&, StoreStatistics* stats) {
        auto t1 = wtime();
        auto key_str = makeKey(label, value);
        std::string val_str;
        serializeValue(value, val_str);
        auto t2 = wtime();
        auto result = storeRawData(batch, key_str, val_str.data(), val_str.size());
        auto t3 = wtime();
        if(stats) {
            stats->serialization_time.updateWith(t2-t1);
            stats->raw_storage_time.updateWith(t3-t2);
        }
        return result;
    }

    /**
     * @brief Implementation of the store function with AsyncEngine
     * and the value type is not am std::vector and not a POD.
     */
    template<typename L, typename V>
    ProductID storeImpl(AsyncEngine& async, const L& label, const V& value,
            const std::integral_constant<bool, false>&, StoreStatistics* stats) {
        auto t1 = wtime();
        auto key_str = makeKey(label, value);
        std::string val_str;
        serializeValue(value, val_str);
        auto t2 = wtime();
        auto result = storeRawData(async, key_str, val_str.data(), val_str.size());
        auto t3 = wtime();
        if(stats) {
            stats->serialization_time.updateWith(t2-t1);
            stats->raw_storage_time.updateWith(t3-t2);
        }
        return result;
    }

    /**
     * @brief Implementation of the store function when the value
     * type is a POD.
     */
    template<typename L, typename V>
    ProductID storeImpl(const L& label, const V& value,
            const std::integral_constant<bool, true>&, StoreStatistics* stats) {
        auto t1 = wtime();
        auto key_str = makeKey(label, value);
        auto t2 = wtime();
        auto result = storeRawData(key_str, reinterpret_cast<const char*>(&value), sizeof(value));
        auto t3 = wtime();
        if(stats) {
            stats->serialization_time.updateWith(t2-t1);
            stats->raw_storage_time.updateWith(t3-t2);
        }
        return result;
    }

    /**
     * @brief Implementation of the store function with WriteBatch
     * when the value type is a POD.
     */
    template<typename L, typename V>
    ProductID storeImpl(WriteBatch& batch, const L& label, const V& value,
            const std::integral_constant<bool, true>&, StoreStatistics* stats) {
        auto t1 = wtime();
        std::string key_str = makeKey(label, value);
        auto t2 = wtime();
        auto result = storeRawData(batch, key_str, reinterpret_cast<const char*>(&value), sizeof(value));
        auto t3 = wtime();
        if(stats) {
            stats->serialization_time.updateWith(t2-t1);
            stats->raw_storage_time.updateWith(t3-t2);
        }
        return result;
    }

    /**
     * @brief Implementation of the store function with AsyncEngine
     * when the value type is a POD.
     */
    template<typename L, typename V>
    ProductID storeImpl(AsyncEngine& async, const L& label, const V& value,
            const std::integral_constant<bool, true>&, StoreStatistics* stats) {
        auto t1 = wtime();
        auto key_str = makeKey(label, value);
        auto t2 = wtime();
        auto result = storeRawData(async, key_str, reinterpret_cast<const char*>(&value), sizeof(value));
        auto t3 = wtime();
        if(stats) {
            stats->serialization_time.updateWith(t2-t1);
            stats->raw_storage_time.updateWith(t3-t2);
        }
        return result;
    }

    /**
     * @brief Implementation of the load function when the value type is a POD.
     */
    template<typename L, typename V>
    bool loadImpl(const L& label, V& value,
            const std::integral_constant<bool, true>&, LoadStatistics* stats) const {
        std::string buffer;
        auto key = makeKey(label, value);
        size_t vsize = sizeof(value);
        auto t1 = wtime();
        auto b = loadRawData(key, reinterpret_cast<char*>(&value), &vsize);
        auto t2 = wtime();
        if(b && stats) {
            stats->deserialization_time.updateWith(0.0);
            stats->raw_loading_time.updateWith(t2-t1);
        }
        return b && (vsize == sizeof(value));
    }

    /**
     * @brief Implementation of the load function with a prefetcher.
     */
    template<typename L, typename V>
    bool loadImpl(const Prefetcher& prefetcher, const L& label, V& value,
            const std::integral_constant<bool, true>&, LoadStatistics* stats) const {
        std::string buffer;
        auto key = makeKey(label, value);
        size_t vsize = sizeof(value);
        auto t1 = wtime();
        bool b = loadRawData(prefetcher, key, reinterpret_cast<char*>(&value), &vsize);
        auto t2 = wtime();
        if(b && stats) {
            stats->deserialization_time.updateWith(0.0);
            stats->raw_loading_time.updateWith(t2-t1);
        }
        return b && (vsize == sizeof(value));
    }

    /**
     * @brief Implementation of the load function with a cache.
     */
    template<typename L, typename V>
    bool loadImpl(const ProductCache& cache, const L& label, V& value,
            const std::integral_constant<bool, true>&, LoadStatistics* stats) const {
        std::string buffer;
        auto key = makeKey(label, value);
        size_t vsize = sizeof(value);
        auto t1 = wtime();
        auto b = loadRawData(cache, key, reinterpret_cast<char*>(&value), &vsize);
        auto t2 = wtime();
        if(b && stats) {
            stats->deserialization_time.updateWith(0.0);
            stats->raw_loading_time.updateWith(t2-t1);
        }
        return b && (vsize == sizeof(value));
    }

    /**
     * @brief Implementation of the load function when the value type is not a POD.
     */
    template<typename L, typename V>
    bool loadImpl(const L& label, V& value,
            const std::integral_constant<bool, false>&,
            LoadStatistics* stats) const {
        std::string buffer;
        auto key = makeKey(label, value);
        auto t1 = wtime();
        auto b = loadRawData(key, buffer);
        auto t2 = wtime();
        if(!b) return false;
        try {
            std::stringstream ss(buffer);
            InputArchive ia(datastore(), ss);
            ia >> value;
        } catch(const std::exception& e) {
            throw Exception(std::string("Exception occured during serialization: ") + e.what());
        }
        auto t3 = wtime();
        if(stats) {
            stats->deserialization_time.updateWith(t2-t1);
            stats->raw_loading_time.updateWith(t3-t2);
        }
        return true;
    }

    /**
     * @brief Implementation of the load function with a prefetcher.
     */
    template<typename L, typename V>
    bool loadImpl(const Prefetcher& prefetcher, const L& label, V& value,
            const std::integral_constant<bool, false>&,
            LoadStatistics* stats) const {
        std::string buffer;
        auto key = makeKey(label, value);
        auto t1 = wtime();
        if(!loadRawData(prefetcher, key, buffer)) {
            return false;
        }
        auto t2 = wtime();
        try {
            std::stringstream ss(buffer);
            InputArchive ia(datastore(), ss);
            ia >> value;
        } catch(const std::exception& e) {
            throw Exception(std::string("Exception occured during serialization: ") + e.what());
        }
        auto t3 = wtime();
        if(stats) {
            stats->deserialization_time.updateWith(t2-t1);
            stats->raw_loading_time.updateWith(t3-t2);
        }
        return true;
    }

    /**
     * @brief Implementation of the load function with a prefetcher.
     */
    template<typename L, typename V>
    bool loadImpl(const ProductCache& cache, const L& label, V& value,
            const std::integral_constant<bool, false>&, LoadStatistics* stats) const {
        std::string buffer;
        auto key = makeKey(label, value);
        auto t1 = wtime();
        if(!loadRawData(cache, key, buffer)) {
            return false;
        }
        auto t2 = wtime();
        try {
            std::stringstream ss(buffer);
            InputArchive ia(datastore(), ss);
            ia >> value;
        } catch(const std::exception& e) {
            throw Exception(std::string("Exception occured during serialization: ") + e.what());
        }
        auto t3 = wtime();
        if(stats) {
            stats->deserialization_time.updateWith(t2-t1);
            stats->raw_loading_time.updateWith(t3-t2);
        }
        return true;
    }

    /**
     * @brief Implementation of the load function when the value type is a vector of POD.
     */
    template<typename L, typename V>
    bool loadVectorImpl(const L& label, std::vector<V>& value,
            const std::integral_constant<bool, true>&, LoadStatistics* stats) const {
        std::string buffer;
        auto key = makeKey(label, value);
        auto t1 = wtime();
        if(!loadRawData(key, buffer)) {
            return false;
        }
        auto t2 = wtime();
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
        auto t3 = wtime();
        if(stats) {
            stats->deserialization_time.updateWith(t2-t1);
            stats->raw_loading_time.updateWith(t3-t2);
        }
        return true;
    }

    /**
     * @brief Implementation of the load function with a prefetcher.
     */
    template<typename L, typename V>
    bool loadVectorImpl(const Prefetcher& prefetcher, const L& label, std::vector<V>& value,
            const std::integral_constant<bool, true>&, LoadStatistics* stats) const {
        std::string buffer;
        auto key = makeKey(label, value);
        auto t1 = wtime();
        if(!loadRawData(prefetcher, key, buffer)) {
            return false;
        }
        auto t2 = wtime();
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
        auto t3 = wtime();
        if(stats) {
            stats->deserialization_time.updateWith(t2-t1);
            stats->raw_loading_time.updateWith(t3-t2);
        }
        return true;
    }

    /**
     * @brief Implementation of the load function with a cache.
     */
    template<typename L, typename V>
    bool loadVectorImpl(const ProductCache& cache, const L& label, std::vector<V>& value,
            const std::integral_constant<bool, true>&, LoadStatistics* stats) const {
        std::string buffer;
        auto key = makeKey(label, value);
        auto t1 = wtime();
        if(!loadRawData(cache, key, buffer)) {
            return false;
        }
        auto t2 = wtime();
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
        auto t3 = wtime();
        if(stats) {
            stats->deserialization_time.updateWith(t2-t1);
            stats->raw_loading_time.updateWith(t3-t2);
        }
        return true;
    }

    /**
     * @brief Implementation of the load function when the value type is a vector of non-POD.
     */
    template<typename L, typename V>
    bool loadVectorImpl(const L& label, std::vector<V>& value,
            const std::integral_constant<bool, false>&, LoadStatistics* stats) const {
        std::string buffer;
        auto key = makeKey(label, value);
        auto t1 = wtime();
        if(!loadRawData(key, buffer)) {
            return false;
        }
        auto t2 = wtime();
        try {
            std::stringstream ss(buffer);
            InputArchive ia(datastore(), ss);
            size_t count = 0;
            ia >> count;
            value.resize(count);
            for(unsigned i=0; i<count; i++) {
                ia >> value[i];
            }
        } catch(const std::exception& e) {
            throw Exception(std::string("Exception occured during serialization: ") + e.what());
        }
        auto t3 = wtime();
        if(stats) {
            stats->deserialization_time.updateWith(t2-t1);
            stats->raw_loading_time.updateWith(t3-t2);
        }
        return true;
    }

    /**
     * @brief Implementation of the load function with a prefetcher.
     */
    template<typename L, typename V>
    bool loadVectorImpl(const Prefetcher& prefetcher, const L& label, std::vector<V>& value,
            const std::integral_constant<bool, false>&, LoadStatistics* stats) const {
        std::string buffer;
        auto key = makeKey(label, value);
        auto t1 = wtime();
        if(!loadRawData(prefetcher, key, buffer)) {
            return false;
        }
        auto t2 = wtime();
        try {
            std::stringstream ss(buffer);
            InputArchive ia(datastore(), ss);
            size_t count = 0;
            ia >> count;
            value.resize(count);
            for(unsigned i=0; i<count; i++) {
                ia >> value[i];
            }
        } catch(const std::exception& e) {
            throw Exception(std::string("Exception occured during serialization: ") + e.what());
        }
        auto t3 = wtime();
        if(stats) {
            stats->deserialization_time.updateWith(t2-t1);
            stats->raw_loading_time.updateWith(t3-t2);
        }
        return true;
    }

    /**
     * @brief Implementation of the load function with a prefetcher.
     */
    template<typename L, typename V>
    bool loadVectorImpl(const ProductCache& cache, const L& label, std::vector<V>& value,
            const std::integral_constant<bool, false>&, LoadStatistics* stats) const {
        std::string buffer;
        auto key = makeKey(label, value);
        auto t1 = wtime();
        if(!loadRawData(cache, key, buffer)) {
            return false;
        }
        auto t2 = wtime();
        try {
            std::stringstream ss(buffer);
            InputArchive ia(datastore(), ss);
            size_t count = 0;
            ia >> count;
            value.resize(count);
            for(unsigned i=0; i<count; i++) {
                ia >> value[i];
            }
        } catch(const std::exception& e) {
            throw Exception(std::string("Exception occured during serialization: ") + e.what());
        }
        auto t3 = wtime();
        if(stats) {
            stats->deserialization_time.updateWith(t2-t1);
            stats->raw_loading_time.updateWith(t3-t2);
        }
        return true;
    }

    /**
     * @brief Creates the string key based on the provided key
     * and the type of the value.
     */
    template<typename L, typename V>
    static std::string makeKey(const L& label, const V& value) {
        return std::string(label) + "#" + demangle<V>();
    }

    /**
     * @brief Creates the string key based on the provided key
     * and the type of the value. Serializes the value into a string.
     */
    template<typename V>
    static void serializeValue(const V& value, std::string& value_str) {
        value_str.resize(0);
        OutputStringWrapper value_wrapper(value_str);
        OutputStream value_stream(value_wrapper);
        OutputArchive oa(value_stream);
        try {
            oa << value;
        } catch(const std::exception& e) {
            throw Exception(std::string("Exception occured during serialization: ") + e.what());
        }
    }

    /**
     * @brief Version of serializeValue for vectors of non-POD datatypes.
     */
    template<typename V>
    static void serializeValueVector(const std::integral_constant<bool, false>&,
            const std::vector<V>& value, std::string& value_str, int start, int end) {
        if(end == -1)
            end = value.size();
        if(start < 0 || start > end || end > value.size())
            throw Exception("Invalid range when storing vector");
        value_str.resize(0);
        OutputStringWrapper value_wrapper(value_str);
        OutputStream value_stream(value_wrapper);
        OutputArchive oa(value_stream);
        try {
            size_t count = end-start;
            oa << count;
            for(auto i = start; i < end; i++)
                oa << value[i];
        } catch(const std::exception& e) {
            throw Exception(std::string("Exception occured during serialization: ") + e.what());
        }
    }

    /**
     * @brief Version of serializeValue for vectors of POD datatypes.
     */
    template<typename V>
    static void serializeValueVector(const std::integral_constant<bool, true>&,
            const std::vector<V>& value, std::string& value_str, int start, int end) {
        if(end == -1)
            end = value.size();
        if(start < 0 || start > end || end > value.size())
            throw Exception("Invalid range when storing vector");
        size_t count = end-start;
        value_str.resize(sizeof(count) + count*sizeof(V));
        std::memcpy(const_cast<char*>(value_str.data()), &count, sizeof(count));
        std::memcpy(const_cast<char*>(value_str.data())+sizeof(count), &value[start], count*sizeof(V));
    }
};

}

#endif
