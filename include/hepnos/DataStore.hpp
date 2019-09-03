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

namespace hepnos {

class ProductID;
class DataSet;
class RunSet;
class Run;
class SubRun;
class Event;
template<typename T, typename C = std::vector<T>> class Ptr;
class WriteBatch;

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

    public:

    typedef DataSet value_type;

    /**
     * @brief Constructor. Initializes the DataStore by taking
     * the name of the configuration file from the environment
     * variable HEPNOS_CONFIG_FILE.
     */
    DataStore();

    /**
     * @brief Constructor. Initializes the DataStore using a YAML
     * configuration file (this file is generated by the HEPnOS
     * service when started.
     *
     * @param configFile Path to a YAML configuration file.
     */
    DataStore(const std::string& configFile);

    /**
     * @brief Copy constructor is deleted.
     */
    DataStore(const DataStore&) = delete;

    /**
     * @brief Move constructor.
     *
     * @param other DataStore to move from.
     */
    DataStore(DataStore&& other);

    /**
     * @brief Copy-assignment operator. Deleted.
     */
    DataStore& operator=(const DataStore&) = delete;

    /**
     * @brief Move-assignment operator.
     *
     * @param other DataStore to move from.
     *
     * @return This DataStore.
     */
    DataStore& operator=(DataStore&& other);
    
    /**
     * @brief Destructor.
     */
    ~DataStore();

    /**
     * @brief Accesses an existing DataSet using the []
     * operator. If no DataSet correspond to the provided name,
     * the function returns a DataSet instance d such that
     * d.valid() is false.
     *
     * @param datasetName Name of the DataSet to retrieve.
     *
     * @return a DataSet corresponding to the provided name.
     */
    DataSet operator[](const std::string& datasetName) const;

    /**
     * @brief iterator class to navigate DataSets.
     * This iterator is a forward iterator. DataSets are sorted
     * alphabetically inside the DataStore.
     */
    class iterator;

    /**
     * @brief const_iterator class to navigate DataSets.
     * This iterator is a forward iterator. DataSets are sorted
     * alphabetically inside the DataStore.
     */
    class const_iterator;

    /**
     * @brief Searches the DataStore for an DataSet with 
     * the provided path and returns an iterator to it if found,
     * otherwise it returns an iterator to DataStore::end().
     *
     * @param datasetPath Path of the DataSet to find.
     *
     * @return an iterator pointing to the DataSet if found,
     * DataStore::end() otherwise.
     */
    iterator find(const std::string& datasetPath);

    /**
     * @brief Searches the DataStore for an DataSet with 
     * the provided path and returns a const_iterator to it 
     * if found, otherwise it returns an iterator to DataStore::end().
     *
     * @param datasetPath Path of the DataSet to find.
     *
     * @return a const_iterator pointing to the DataSet if found,
     * DataStore::cend() otherwise.
     */
    const_iterator find(const std::string& datasetPath) const;

    /**
     * @brief Returns an iterator referring to the first DataSet
     * in the DataStore.
     *
     * @return an iterator referring to the first DataSet in the DataStore.
     */
    iterator begin();

    /**
     * @brief Returns an iterator referring to the end of the DataStore.
     * The DataSet pointed to by this iterator is not valid (that is,
     * `end()->valid()` returns `false`).
     *
     * @return an iterator referring to the end of the DataStore.
     */
    iterator end();

    /**
     * @brief Returns a const_iterator referring to the first DataSet
     * in the DataStore.
     *
     * @return a const_iterator referring to the first DataSet in the DataStore.
     */
    const_iterator begin() const;

    /**
     * @brief Returns a const_iterator referring to the end of the DataStore.
     * The DataSet pointed to by this iterator is not valid (that is,
     * `end()->valid()` returns `false`).
     *
     * @return a const_iterator referring to the end of the DataStore.
     */
    const_iterator end() const;

    /**
     * @brief Returns a const_iterator referring to the first DataSet
     * in the DataStore.
     *
     * @return a const_iterator referring to the first DataSet in the DataStore.
     */
    const_iterator cbegin() const;

    /**
     * @brief Returns a const_iterator referring to the end of the DataStore.
     * The DataSet pointed to by this iterator is not valid (that is,
     * `cend()->valid()` return `false`).
     *
     * @return a const_iterator referring to the end of the DataStore.
     */
    const_iterator cend() const;

    /**
     * @brief Returns an iterator pointing to the first DataSet in the 
     * DataStore whose name is not considered to go before lb 
     * (i.e., either it is equal or goes after, alphabetically).
     *
     * @param lb DataSet name to search for.
     *
     * @return An iterator to the the first DataSet in the DataStore 
     * whose name is not considered to go before lb, or DataStore::end() 
     * if all keys are considered to go before it.
     */
    iterator lower_bound(const std::string& lb);

    /**
     * @brief Returns a const_iterator pointing to the first DataSet in the 
     * DataStore whose name is not considered to go before lb 
     * (i.e., either it is equal or goes after, alphabetically).
     *
     * @param lb DataSet name to search for.
     *
     * @return A const_iterator to the the first DataSet in the DataStore 
     * whose name is not considered to go before lb, or DataStore::cend() 
     * if all DataSet names are considered to go before it.
     */
    const_iterator lower_bound(const std::string& lb) const;

    /**
     * @brief Returns an iterator pointing to the first DataSet in the 
     * DataStore whose key is considered to go after ub.
     *
     * @param ub DataSet name to search for.
     *
     * @return An iterator to the the first DataSet in the DataStore 
     * whose name is considered to go after ub, or DataStore::end() if 
     * no DataSet names are considered to go after it.
     */
    iterator upper_bound(const std::string& ub);

    /**
     * @brief Returns a const_iterator pointing to the first DataSet in the 
     * DataStore whose key is considered to go after ub.
     *
     * @param ub DataSet name to search for.
     *
     * @return A const_iterator to the the first DataSet in the DataStore 
     * whose name is considered to go after ub, or DataStore::end() if 
     * no DataSet names are considered to go after it.
     */
    const_iterator upper_bound(const std::string& ub) const;

    /**
     * @brief Creates a dataset with a given name inside the
     * DataStore. This name must not have the '/' and '%' characters.
     * A DataSet object pointing to the created dataset is returned.
     * If a dataset with this name already exists in the DataStore, 
     * it is not created, but a DataSet object pointing to the 
     * existing one is returned instead.
     *
     * @param name Name of DataSet.
     *
     * @return A DataSet instance pointing to the created dataset.
     */
    DataSet createDataSet(const std::string& name);

    /**
     * @brief Creates a dataset with a given name inside the data store.
     * This function takes a WriteBatch instance, the dataset will be
     * actually created when this batch is flushed or destroyed.
     *
     * @param batch WriteBatch in which to enqueue the creation.
     * @param name Name of the dataset.
     *
     * @return A DataSet instance pointing to the created dataset.
     */
    DataSet createDataSet(WriteBatch& batch, const std::string& name);

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
     * @brief Shuts down the HEPnOS service.
     */
    void shutdown();

    private:

    /**
     * @brief Implementation of the class (using Pimpl idiom)
     */
    class Impl;
    std::unique_ptr<Impl> m_impl; /*!< Pointer to implementation */

    /**
     * @brief Loads the raw data corresponding to a product.
     *
     * @param productID Product id.
     * @param buffer Buffer in which to place the raw data.
     *
     * @return true if the data was loaded successfuly, false otherwise.
     */
    bool loadRawProduct(const ProductID& productID, std::string& buffer);
};

class DataStore::const_iterator {

    protected:

    /**
     * @brief Implementation of the class (using Pimpl idiom)
     */
    class Impl;
    std::unique_ptr<Impl> m_impl; /*!< Pointer to implementation */

    public:
    /**
     * @brief Constructor. Creates a const_iterator pointing
     * to an invalid DataSet.
     */
    const_iterator();

    /**
     * @brief Constructor. Creates a const_iterator pointing
     * to a given DataSet. The DataSet may or may not be valid. 
     *
     * @param current DataSet to make the const_iterator point to.
     */
    const_iterator(const DataSet& current);

    /**
     * @brief Constructor. Creates a const_iterator pointing
     * to a given DataSet. The DataSet may or may not be valid. 
     *
     * @param current DataSet to make the const_iterator point to.
     */
    const_iterator(DataSet&& current);

    typedef const_iterator self_type;
    typedef DataSet value_type;
    typedef DataSet& reference;
    typedef DataSet* pointer;
    typedef int difference_type;
    typedef std::forward_iterator_tag iterator_category;

    /**
     * @brief Destructor. This destructor is virtual because
     * the iterator class inherits from const_iterator.
     */
    virtual ~const_iterator();

    /**
     * @brief Copy-constructor.
     *
     * @param other const_iterator to copy.
     */
    const_iterator(const const_iterator& other);

    /**
     * @brief Move-constructor.
     *
     * @param other const_iterator to move.
     */
    const_iterator(const_iterator&& other);

    /**
     * @brief Copy-assignment operator.
     *
     * @param other const_iterator to copy.
     *
     * @return this.
     */
    const_iterator& operator=(const const_iterator&);

    /**
     * @brief Move-assignment operator.
     *
     * @param other const_iterator to move.
     *
     * @return this.
     */
    const_iterator& operator=(const_iterator&&);

    /**
     * @brief Increments the const_iterator, returning
     * a copy of the iterator after incrementation.
     *
     * @return a copy of the iterator after incrementation.
     */
    self_type operator++();

    /**
     * @brief Increments the const_iterator, returning
     * a copy of the iterator before incrementation.
     *
     * @return a copy of the iterator after incrementation.
     */
    self_type operator++(int);

    /**
     * @brief Dereference operator. Returns a const reference
     * to the DataSet this const_iterator points to.
     *
     * @return a const reference to the DataSet this 
     *      const_iterator points to.
     */
    const reference operator*();

    /**
     * @brief Returns a const pointer to the DataSet this
     * const_iterator points to.
     *
     * @return a const pointer to the DataSet this 
     *      const_iterator points to.
     */
    const pointer operator->();

    /**
     * @brief Compares two const_iterators. The two const_iterators
     * are equal if they point to the same DataSet or if both
     * correspond to DataStore::cend().
     *
     * @param rhs const_iterator to compare with.
     *
     * @return true if the two const_iterators are equal, false otherwise.
     */
    bool operator==(const self_type& rhs) const;

    /**
     * @brief Compares two const_iterators.
     *
     * @param rhs const_iterator to compare with.
     *
     * @return true if the two const_iterators are different, false otherwise.
     */
    bool operator!=(const self_type& rhs) const;
};

class DataStore::iterator : public DataStore::const_iterator {

    public:

    /**
     * @brief Constructor. Builds an iterator pointing to an
     * invalid DataSet.
     */
    iterator();

    /**
     * @brief Constructor. Builds an iterator pointing to
     * an existing DataSet. The DataSet may or may not be
     * valid.
     *
     * @param current DataSet to point to.
     */
    iterator(const DataSet& current);

    /**
     * @brief Constructor. Builds an iterator pointing to
     * an existing DataSet. The DataSet may or may not be
     * valid.
     *
     * @param current DataSet to point to.
     */
    iterator(DataSet&& current);

    typedef iterator self_type;
    typedef DataSet value_type;
    typedef DataSet& reference;
    typedef DataSet* pointer;
    typedef int difference_type;
    typedef std::forward_iterator_tag iterator_category;

    /**
     * @brief Destructor.
     */
    ~iterator();

    /**
     * @brief Copy constructor.
     *
     * @param other iterator to copy.
     */
    iterator(const iterator& other);

    /**
     * @brief Move constructor.
     *
     * @param other iterator to move.
     */
    iterator(iterator&& other);

    /**
     * @brief Copy-assignment operator.
     *
     * @param other iterator to copy.
     *
     * @return this.
     */
    iterator& operator=(const iterator& other);

    /**
     * @brief Move-assignment operator.
     *
     * @param other iterator to move.
     *
     * @return this.
     */
    iterator& operator=(iterator&& other);

    /**
     * @brief Dereference operator. Returns a reference
     * to the DataSet this iterator points to.
     *
     * @return A reference to the DataSet this iterator
     *      points to.
     */
    reference operator*();

    /**
     * @brief Returns a pointer to the DataSet this iterator
     * points to.
     *
     * @return A pointer to the DataSet this iterator points to.
     */
    pointer operator->();
};

}

#include <hepnos/ProductID.hpp>
#include <hepnos/Ptr.hpp>

namespace hepnos {

template<typename T>
Ptr<T> DataStore::makePtr(const ProductID& productID) {
    return Ptr<T>(this, productID);
}

template<typename T, typename C = std::vector<T>>
Ptr<T,C> DataStore::makePtr(const ProductID& productID, std::size_t index) {
    return Ptr<T,C>(this, productID, index);
}

template<typename T>
bool DataStore::loadProduct(const ProductID& productID, T& t) {
    std::string buffer;
    if(!loadRawProduct(productID, buffer)) {
        return false;
    }
    std::string serialized(buffer.begin(), buffer.end());
    std::stringstream ss(serialized);
    InputArchive ia(this, ss);
    try {
        ia >> t;
    } catch(...) {
        throw Exception("Exception occured during serialization");
    }
    return true;
}

}

#endif
