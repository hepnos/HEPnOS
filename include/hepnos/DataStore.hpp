/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_DATA_STORE_H
#define __HEPNOS_DATA_STORE_H

#include <vector>
#include <string>
#include <memory>
#include <hepnos/ItemType.hpp>

namespace hepnos {

class ProductID;
class DataSet;
class RunSet;
class Run;
class SubRun;
class Event;
class EventSet;
class DataStoreImpl;
class DataSetImpl;
class RunSetImpl;
class ItemImpl;
template<typename T, typename C = std::vector<T>> class Ptr;
class WriteBatch;
class AsyncEngine;
class ParallelEventProcessor;
class ParallelEventProcessorImpl;
class Prefetcher;

/**
 * The DataStore class is the main handle referencing an HEPnOS service.
 * It provides functionalities to navigate DataSets.
 */
class DataStore {

    friend class ProductID;
    friend class DataSet;
    friend class RunSet;
    friend class Run;
    friend class SubRun;
    friend class Event;
    friend class WriteBatch;
    friend class DataStoreImpl;
    friend class AsyncEngine;
    friend class EventSet;
    friend class ParallelEventProcessor;
    friend class ParallelEventProcessorImpl;
    friend class Prefetcher;

    public:

    /**
     * @brief Constructor. Initializes the DataStore by taking
     * the name of the configuration file from the environment
     * variable HEPNOS_CONFIG_FILE.
     *
     * @param use_progress_thread whether to use a background thread for networking.
     */
    static DataStore connect(bool use_progress_thread=false);

    /**
     * @brief Constructor. Initializes the DataStore using a YAML
     * configuration file (this file is generated by the HEPnOS
     * service when started.
     *
     * @param configFile Path to a YAML configuration file.
     * @param use_progress_thread whether to use a background thread for networking.
     */
    static DataStore connect(const std::string& configFile, bool use_progress_thread=false);

    /**
     * @brief Default constructor (for an invalid DataStore not yet initialized).
     */
    DataStore() = default;

    /**
     * @brief Copy constructor.
     */
    DataStore(const DataStore&) = default;

    /**
     * @brief Move constructor.
     *
     * @param other DataStore to move from.
     */
    DataStore(DataStore&& other) = default;

    /**
     * @brief Copy-assignment operator.
     */
    DataStore& operator=(const DataStore&) = default;

    /**
     * @brief Move-assignment operator.
     *
     * @param other DataStore to move from.
     *
     * @return This DataStore.
     */
    DataStore& operator=(DataStore&& other) = default;

    /**
     * @brief Destructor.
     */
    ~DataStore() = default;

    /**
     * @brief Indicated whether the DataStore is valid (i.e. connected).
     */
    bool valid() const;

    /**
     * @brief Get the root of the DataStore as a DataSet instance.
     */
    DataSet root() const;

    /**
     * @brief Create a pointer to a product. The type T used must
     * match the type of the product corresponding to the provided
     * product ID. Using a different type will result in undefined
     * behaviors.
     *
     * @tparam T Type of the product.
     * @param productID Product ID.
     *
     * @return a Ptr instance pointing to the given product.
     */
    template<typename T>
    Ptr<T> makePtr(const ProductID& productID);

    /**
     * @brief Create a pointer to a product located in a collection
     * at a given index. The type T used must match the type of objects
     * stored in the collection. Using a different type will result in
     * undefined behavior. The collection type may be ommited if it
     * is std::vector<T>. The collection type must have a braket operator
     * taking an unsigned integral type (e.g. size_t) as input and returning
     * a const reference (or a reference) to an object of type T.
     *
     * @tparam T Type of object pointed to.
     * @tparam C Type of collection.
     * @param productID Product id.
     * @param index Index of the object in the collection.
     *
     * @return a Ptr instance pointing to the given product.
     */
    template<typename T, typename C = std::vector<T>>
    Ptr<T,C> makePtr(const ProductID& productID, std::size_t index);

    /**
     * @brief Loads a particular object by its product id.
     *
     * @tparam T Type of object.
     * @param productID Product id.
     * @param t Object being loaded.
     *
     * @return True if the object was correctly loaded, false otherwise.
     */
    template<typename T>
    bool loadProduct(const ProductID& productID, T& t);

    /**
     * @brief Specialization of loadProduct for vectors.
     */
    template<typename T>
    bool loadProduct(const ProductID& productID, std::vector<T>& t);

    /**
     * @brief Shuts down the HEPnOS service.
     */
    void shutdown();

    /**
     * @brief Returns the number of underlying targets for the
     * specified item type.
     *
     * @param type Item type.
     *
     * @return The number of targets for the item type.
     */
    size_t numTargets(const ItemType& type) const;

    private:

    std::shared_ptr<DataStoreImpl> m_impl; /*!< Pointer to implementation */

    /**
     * @brief Constructor from a pointer to implementation.
     *
     * @param impl
     */
    DataStore(std::shared_ptr<DataStoreImpl>&& impl);
    DataStore(const std::shared_ptr<DataStoreImpl>& impl);

    /**
     * @brief Loads the raw data corresponding to a product.
     *
     * @param productID Product id.
     * @param buffer Buffer in which to place the raw data.
     *
     * @return true if the data was loaded successfuly, false otherwise.
     */
    bool loadRawProduct(const ProductID& productID, std::string& buffer);

    /**
     * @brief Loads the raw data corresponding to a product into a buffer.
     *
     * @param productID Product id.
     * @param value Buffer in which to place the raw data.
     * @param value_size Size of the input buffer, set to size of the actual data after the call.
     *
     * @return true if the data was loaded successfuly, false otherwise.
     */
    bool loadRawProduct(const ProductID& productID, char* value, size_t* value_size);

    /**
     * @brief Loads the raw data of a product directly into the product itself,
     * with the product type T not a POD type.
     *
     * @tparam T Type of the product.
     * @param productID Product id.
     * @param t Product.
     * @param std::integral_constant type trait indicating T is non-POD.
     *
     * @return true if the data was loaded successfuly, false otherwise.
     */
    template<typename T>
    bool loadProductImpl(const ProductID& productID, T& t, const std::integral_constant<bool, false>&);

    /**
     * @brief Loads the raw data of a product directly into the product itself,
     * with the product type T being a POD type.
     *
     * @tparam T Type of the product.
     * @param productID Product id.
     * @param t Product.
     * @param std::integral_constant type trait indicating T is POD.
     *
     * @return true if the data was loaded successfuly, false otherwise.
     */
    template<typename T>
    bool loadProductImpl(const ProductID& productID, T& t, const std::integral_constant<bool, true>&);

    /**
     * @brief Loads the raw data of a product with the product type being an std::vector
     * of non-POD type.
     *
     * @tparam T Type of vector elements.
     * @param productID Product id.
     * @param t Product.
     * @param std::integral_constant type trait indicating T is non-POD.
     *
     * @return true if the data was loaded successfuly, false otherwise.
     */
    template<typename T>
    bool loadProductImpl(const ProductID& productID, std::vector<T>& t, const std::integral_constant<bool, false>&);

    /**
     * @brief Loads the raw data of a product with the product type being an std::vector
     * of POD type.
     *
     * @tparam T Type of vector elements.
     * @param productID Product id.
     * @param t Product.
     * @param std::integral_constant type trait indicating T is POD.
     *
     * @return true if the data was loaded successfuly, false otherwise.
     */
    template<typename T>
    bool loadProductImpl(const ProductID& productID, std::vector<T>& t, const std::integral_constant<bool, true>&);
};

}

#include <hepnos/ProductID.hpp>
#include <hepnos/Ptr.hpp>

namespace hepnos {

template<typename T>
Ptr<T> DataStore::makePtr(const ProductID& productID) {
    return Ptr<T>(*this, productID);
}

template<typename T, typename C = std::vector<T>>
Ptr<T,C> DataStore::makePtr(const ProductID& productID, std::size_t index) {
    return Ptr<T,C>(*this, productID, index);
}

template<typename T>
bool DataStore::loadProduct(const ProductID& productID, T& t) {
    return loadProductImpl(productID, t, std::is_pod<T>());
}

template<typename T>
bool DataStore::loadProduct(const ProductID& productID, std::vector<T>& t) {
    return loadProductImpl(productID, t, std::is_pod<T>());
}

template<typename T>
bool DataStore::loadProductImpl(const ProductID& productID, T& t, const std::integral_constant<bool, false>&) {
    std::string buffer;
    if(!loadRawProduct(productID, buffer)) {
        return false;
    }
    std::stringstream ss(buffer);
    InputArchive ia(*this, ss);
    try {
        ia >> t;
    } catch(const std::exception& e) {
        throw Exception(std::string("Exception occured during serialization: ") + e.what());
    }
    return true;
}

template<typename T>
bool DataStore::loadProductImpl(const ProductID& productID, T& t, const std::integral_constant<bool, true>&) {
    size_t value_size = sizeof(t);
    if(loadRawProduct(productID, reinterpret_cast<char*>(&t), &value_size)) {
        return value_size == sizeof(t);
    } else {
        return false;
    }
}

template<typename T>
bool DataStore::loadProductImpl(const ProductID& productID, std::vector<T>& t, const std::integral_constant<bool, false>&) {
    std::string buffer;
    if(!loadRawProduct(productID, buffer)) {
        return false;
    }
    std::stringstream ss(buffer);
    InputArchive ia(*this, ss);
    try {
        size_t count = 0;
        ia >> count;
        t.resize(count);
        for(unsigned i=0; i < count; i++) {
            ia >> t[i];
        }
    } catch(const std::exception& e) {
        throw Exception(std::string("Exception occured during serialization: ") + e.what());
    }
    return true;
}

template<typename T>
bool DataStore::loadProductImpl(const ProductID& productID, std::vector<T>& t, const std::integral_constant<bool, true>&) {
    std::string buffer;
    if(!loadRawProduct(productID, buffer)) {
        return false;
    }
    size_t count = 0;
    if(buffer.size() < sizeof(count)) return false;
    std::memcpy(&count, buffer.data(), sizeof(count));
    if(buffer.size() != sizeof(count) + count*sizeof(T)) return false;
    t.resize(count);
    std::memcpy(t.data(), buffer.data() + sizeof(count), sizeof(T)*count);
    return true;
}

}

#endif
