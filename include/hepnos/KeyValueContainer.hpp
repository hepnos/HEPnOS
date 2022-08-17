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

    template<typename T, typename _ = void>
    struct IsVector {
        static const bool value = false;
    };

    template<typename T>
    struct IsVector<T,
        std::enable_if_t<
            std::is_same<T, std::vector<typename T::value_type, typename T::allocator_type>>::value
        >> {
        static const bool value = true;
    };


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
     * @brief Stores a key/value pair into the KeyValueContainer.
     * The type of key should be convertible into an std::string.
     * The resulting string must not have the "/", or "%" characters.
     * The type of the value must be serializable using Boost.
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
    std::enable_if_t<!IsVector<V>::value, ProductID>
    store(const L& label, const V& value, StoreStatistics* stats = nullptr) {
        auto ds = datastore();
        return store(ds, label, value, stats);
    }

    /**
     * @brief Stores a key/value pair into the Target
     * (WriteBatch, AsyncEngine, etc.).
     *
     * Note that since the Target may delay the operation,
     * the returned ProductID is valid even though the operation
     * may not ultimately succeed.
     *
     * @tparam L type of the label.
     * @tparam V type of the value.
     * @tparam Target type of target.
     * @param target Target (WriteBatch, AsyncEngine, etc.).
     * @param label Label to store.
     * @param value Value to store.
     *
     * @return a valid ProductID.
     */
    template<typename L, typename V>
    std::enable_if_t<!IsVector<V>::value, ProductID>
    store(RawStorage& target, const L& label, const V& value,
                    StoreStatistics* stats = nullptr) {
        return storeImpl(target, label, value, std::is_pod<std::remove_reference_t<V>>(), stats);
    }

    /**
     * @brief Version of store when the value is an std::vector.
     */
    template<typename L, typename V>
    ProductID store(const L& label, const std::vector<V>& value, int start=0, int end=-1,
                    StoreStatistics* stats = nullptr) {
        auto ds = datastore();
        return store(ds, label, value, start, end, stats);
    }

    /**
     * @brief Version of store when the value is an std::vector.
     */
    template<typename L, typename V>
    ProductID store(RawStorage& target, const L& label, const std::vector<V>& value, int start=0, int end=-1,
                    StoreStatistics* stats = nullptr) {
        auto t1 = wtime();
        auto key = makeKey(label, value);
        std::string val_str;
        serializeValueVector(std::is_pod<std::remove_reference_t<V>>(), value, val_str, start, end);
        auto t2 = wtime();
        auto result = target.valid() ? target.storeRawData(key, val_str.data(), val_str.size())
                                     : datastore().storeRawData(key, val_str.data(), val_str.size());
        auto t3 = wtime();
        if(stats) {
            stats->serialization_time.updateWith(t2-t1);
            stats->raw_storage_time.updateWith(t3-t2);
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
     * @tparam L type of the label.
     * @tparam V type of the value.
     * @param key Key to load.
     * @param value Value to load.
     *
     * @return true if the key exists and was loaded. False otherwise.
     */
    template<typename L, typename V>
    std::enable_if_t<!IsVector<V>::value, bool>
    load(const L& label, V& value, LoadStatistics* stats = nullptr) const {
        auto ds = datastore();
        return load(ds, label, value, stats);
    }

    /**
     * @brief Version of load for vectors.
     */
    template<typename L, typename V>
    bool load(const L& label, std::vector<V>& value, LoadStatistics* stats = nullptr) const {
        auto ds = datastore();
        return load(ds, label, value,  stats);
    }

    /**
     * @brief Version of load that will first look into the Source
     * argument for the requested key.
     */
    template<typename L, typename V, typename Source>
    std::enable_if_t<!IsVector<V>::value, bool>
    load(const Source& source, const L& label, V& value,
              LoadStatistics* stats = nullptr) const {
        return loadImpl(source, label, value, std::is_pod<std::remove_reference_t<V>>(), stats);
    }

    /**
     * @brief Version of load for vectors, looking first into the
     * Source argument for the requested key.
     */
    template<typename L, typename V, typename Source>
    bool load(const Source& source, const L& label, std::vector<V>& value,
              LoadStatistics* stats = nullptr) const {
        return loadVectorImpl(source, label, value,
                std::is_pod<std::remove_reference_t<V>>(), stats);
    }

    /**
     * @brief List all the product ids contained in this container.
     *
     * @param label Optional label to filter products.
     *
     * @return List of product ids.
     */
    virtual std::vector<ProductID> listProducts(const std::string& label="") const = 0;

    protected:

    /**
     * @brief Create the full product key for this container from the label
     * and type name.
     *
     * @param label Label
     * @param label_size Label size
     * @param type Type name
     * @param type_size Size of type name
     *
     * @return Full product key.
     */
    virtual ProductID makeProductID(const char* label, size_t label_size,
                                    const char* type, size_t type_size) const = 0;

    private:

    /**
     * @brief Implementation of the store function with WriteBatch
     * and the value type is not am std::vector and not a POD.
     */
    template<typename L, typename V>
    ProductID storeImpl(RawStorage& target, const L& label, const V& value,
            const std::integral_constant<bool, false>&, StoreStatistics* stats) {
        auto t1 = wtime();
        auto key = makeKey(label, value);
        std::string val_str;
        serializeValue(value, val_str);
        auto t2 = wtime();
        auto result = target.valid() ? target.storeRawData(key, val_str.data(), val_str.size())
                                     : datastore().storeRawData(key, val_str.data(), val_str.size());
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
    ProductID storeImpl(RawStorage& target, const L& label, const V& value,
            const std::integral_constant<bool, true>&, StoreStatistics* stats) {
        auto t1 = wtime();
        auto key = makeKey(label, value);
        auto t2 = wtime();
        auto result = target.valid() ? target.storeRawData(key, reinterpret_cast<const char*>(&value), sizeof(value))
                                     : datastore().storeRawData(key, reinterpret_cast<const char*>(&value), sizeof(value));
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
    template<typename L, typename V, typename Source>
    bool loadImpl(const Source& source, const L& label, V& value,
            const std::integral_constant<bool, true>&, LoadStatistics* stats) const {
        std::string buffer;
        auto key = makeKey(label, value);
        size_t vsize = sizeof(value);
        auto t1 = wtime();
        auto b = source.valid() ? source.loadRawData(key, reinterpret_cast<char*>(&value), &vsize)
                                : datastore().loadRawData(key, reinterpret_cast<char*>(&value), &vsize);
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
    template<typename L, typename V, typename Source>
    bool loadImpl(const Source& source, const L& label, V& value,
            const std::integral_constant<bool, false>&,
            LoadStatistics* stats) const {
        std::string buffer;
        auto key = makeKey(label, value);
        auto t1 = wtime();
        auto b = source.valid() ? source.loadRawData(key, buffer)
                                : datastore().loadRawData(key, buffer);
        if(!b) {
            return false;
        }
        auto t2 = wtime();
        try {
            InputStringWrapper value_wrapper(buffer.data(), buffer.size());
            InputStream value_stream(value_wrapper);
            InputArchive ia(datastore(), value_stream);
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
     * @brief Implementation of the load function.
     */
    template<typename L, typename V, typename Source>
    bool loadVectorImpl(const Source& source, const L& label, std::vector<V>& value,
            const std::integral_constant<bool, true>&, LoadStatistics* stats) const {
        std::string buffer;
        auto key = makeKey(label, value);
        auto t1 = wtime();
        auto b = source.valid() ? source.loadRawData(key, buffer)
                                : datastore().loadRawData(key, buffer);
        if(!b) {
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
     * @brief Implementation of the load function.
     */
    template<typename L, typename V, typename Source>
    bool loadVectorImpl(const Source& source, const L& label, std::vector<V>& value,
            const std::integral_constant<bool, false>&, LoadStatistics* stats) const {
        std::string buffer;
        auto key = makeKey(label, value);
        auto t1 = wtime();
        auto b = source.valid() ? source.loadRawData(key, buffer)
                                : datastore().loadRawData(key, buffer);
        if(!b) {
            return false;
        }
        auto t2 = wtime();
        try {
            InputStringWrapper value_wrapper(buffer.data(), buffer.size());
            InputStream value_stream(value_wrapper);
            InputArchive ia(datastore(), value_stream);
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
    auto makeKey(const L& l, const V& value) const {
        auto label = std::string{l};
        auto type = std::string{demangle<V>()};
        return makeProductID(label.data(), label.size(), type.data(), type.size());
    }

    /**
     * @brief Creates the string key based on the provided key
     * and the type of the value. Serializes the value into a string.
     */
    template<typename V>
    static void serializeValue(const V& value, std::string& value_str) {
        value_str.resize(0);

        OutputSizer value_sizer;
        OutputSizeEvaluator value_size_evaluator(value_sizer);
        OutputArchive sizing_oa(value_size_evaluator);
        try {
            sizing_oa << value;
        } catch(const std::exception& e) {
            throw Exception(std::string("Exception occured during product size estimation: ") + e.what());
        }

        value_str.reserve(value_sizer.size());

        OutputStringWrapper value_wrapper(value_str);
        OutputStream value_stream(value_wrapper);
        OutputArchive output_oa(value_stream);
        try {
            output_oa << value;
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
        OutputSizer value_sizer;
        OutputSizeEvaluator value_size_evaluator(value_sizer);
        OutputArchive sizing_oa(value_size_evaluator);
        try {
            sizing_oa << value;
        } catch(const std::exception& e) {
            throw Exception(std::string("Exception occured during product size estimation: ") + e.what());
        }
        value_str.reserve(value_sizer.size());

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
