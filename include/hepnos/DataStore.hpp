#ifndef __HEPNOS_DATA_STORE_H
#define __HEPNOS_DATA_STORE_H

#include <vector>
#include <string>
#include <memory>

namespace hepnos {

class DataSet;
class RunSet;
class Run;
class SubRun;

/**
 * The DataStore class is the main handle referencing an HEPnOS service.
 * It provides functionalities to navigate DataSets.
 */
class DataStore {

    friend class DataSet;
    friend class RunSet;
    friend class Run;
    friend class SubRun;

    public:

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
     * the provided name and returns an iterator to it if found,
     * otherwise it returns an iterator to DataStore::end().
     *
     * @param datasetName Name of the DataSet to find.
     *
     * @return an iterator pointing to the DataSet if found,
     * DataStore::end() otherwise.
     */
    iterator find(const std::string& datasetName);

    /**
     * @brief Searches the DataStore for an DataSet with 
     * the provided name and returns a const_iterator to it 
     * if found, otherwise it returns an iterator to DataStore::end().
     *
     * @param datasetName Name of the DataSet to find.
     *
     * @return a const_iterator pointing to the DataSet if found,
     * DataStore::cend() otherwise.
     */
    const_iterator find(const std::string& datasetName) const;

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
     * DataStore. This name must not have the '/' and '#' characters.
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

    private:

    /**
     * @brief Implementation of the class (using Pimpl idiom)
     */
    class Impl;
    std::unique_ptr<Impl> m_impl; /*!< Pointer to implementation */
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
     * @return true if the two const_iterators are different, false atherwise.
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

#endif
