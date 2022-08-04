/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_DATA_SET_H
#define __HEPNOS_DATA_SET_H

#include <memory>
#include <mpi.h>
#include <hepnos/Exception.hpp>
#include <hepnos/RunNumber.hpp>
#include <hepnos/DataStore.hpp>
#include <hepnos/KeyValueContainer.hpp>

namespace hepnos {

class RunSet;
class Run;
class ParallelEventProcessor;

/**
 * @brief The DataSet class represents a handle to a named dataset
 * stored either at the root of an HEPnOS DataStore service, or within
 * another dataset. It provides functionalities to navigate nested
 * datasets and to load/store data products.
 */
class DataSet : public KeyValueContainer {

    friend class DataStore;
    friend class RunSet;
    friend class DataSetImpl;
    friend class ParallelEventProcessor;

    private:

    std::shared_ptr<DataSetImpl> m_impl; /*!< Pointer to implementation. */

    /**
     * @brief Constructor.
     */
    DataSet(const std::shared_ptr<DataSetImpl>& impl);
    DataSet(std::shared_ptr<DataSetImpl>&& impl);

    public:

    typedef DataSet value_type;

    /**
     * @brief Default constructor.
     */
    DataSet();

    /**
     * @brief Copy-constructor.
     *
     * @param other DataSet to copy.
     */
    DataSet(const DataSet& other) = default;

    /**
     * @brief Move-constructor.
     *
     * @param other DataSet to move.
     */
    DataSet(DataSet&& other) = default;

    /**
     * @brief Copy-assignment operator.
     *
     * @param other DataSet to copy.
     *
     * @return this.
     */
    DataSet& operator=(const DataSet& other) = default;

    /**
     * @brief Move-assignment operator.
     *
     * @param other DataSet to move.
     *
     * @return this.
     */
    DataSet& operator=(DataSet&& other) = default;

    /**
     * @brief Destructor.
     */
    ~DataSet() = default;

    /**
     * @brief Overrides datastore from KeyValueContainer class.
     */
    DataStore datastore() const override;

    /**
     * @brief Name of the DataSet.
     *
     * @return the name of the DataSet.
     */
    const std::string& name() const;

    /**
     * @brief Nesting level of the dataset.
     */
    uint8_t level() const;

    /**
     * @brief Name of the container of the DataSet.
     *
     * @return the name of the container of the DataSet.
     */
    const std::string& container() const;

    /**
     * @brief Full name of the DataSet
     * (container() + "/" + name() if container() is not empty,
     * name() otherwise)
     *
     * @return the full name of the DataSet.
     */
    std::string fullname() const;

    /**
     * @brief Gets the next DataSet from this DataSet in
     * alphabetical order within the same container.
     * If no such dataset exists, this function returns
     * a DataSet instance such that valid() == false.
     *
     * @return the next DataSet from this DataSet.
     */
    DataSet next() const;

    /**
     * @brief Check if a DataSet is valid, i.e. if it
     * corresponds to a DataSet that exists in the
     * underlying DataStore.
     *
     * @return true if the DataSet is valid, false otherwise.
     */
    bool valid() const;

    /**
     * @see KeyValueContainer::listProducts()
     */
    std::vector<ProductID> listProducts(const std::string& label="") const;

    /**
     * @brief Comparison operator.
     *
     * @param other DataSet to compare with.
     *
     * @return true of both DataSets point to the same
     * entry in the HEPnOS service, false otherwise.
     */
    bool operator==(const DataSet& other) const;

    /**
     * @brief Comparison operator.
     *
     * @param other DataSet to compare with.
     *
     * @return false of both DataSets point to the same
     * entry in the HEPnOS service, true otherwise.
     */
    bool operator!=(const DataSet& other) const;

    /**
     * @brief Creates a dataset with a given name inside the
     * DataSet. This name must not have the '/' and '%' characters.
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
     * @brief Creates a run with a given run number inside the DataSet.
     * A Run object pointing to the created run is returned.
     * If a run with the same number exists in this DataSet, the run
     * is not created by a Run object pointing to the existing one is
     * returned instead.
     *
     * @param runNumber Run number of the run to create.
     *
     * @return A Run instance pointing to the created run.
     */
    Run createRun(const RunNumber& runNumber);

    /**
     * @brief Creates a Run asynchronously.
     * Note that even though the returned Run object is valid, since
     * the actual creation operation will happen in the background,
     * there is no guarantee that this operation will succeed.
     *
     * @param async AsyncEngine to use to make the operation happen in the background.
     * @param runNumber Run number.
     *
     * @return a valid Run object.
     */
    Run createRun(AsyncEngine& async, const RunNumber& runNumber);

    /**
     * @brief Creates a Run by pushing the creation operation into
     * a WriteBatch. Note that even though the returned Run object is valid,
     * since the actual creation is delayed until the WriteBatch is flushed,
     * there is no guarantee that this operation will ultimately succeed.
     *
     * @param batch WriteBatch in which to append the operation.
     * @param runNumber Run number.
     *
     * @return a valid Run object.
     */
    Run createRun(WriteBatch& batch, const RunNumber& runNumber);

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
     * @brief Searches this DataSet for a DataSet with
     * the provided path and returns an iterator to it if found,
     * otherwise it returns an iterator to DataStore::end().
     *
     * @param datasetPath Path of the DataSet to find.
     *
     * @return an iterator pointing to the DataSet if found,
     * DataSet::end() otherwise.
     */
    iterator find(const std::string& datasetPath);

    /**
     * @brief Searches this DataSet for an DataSet with
     * the provided path and returns a const_iterator to it
     * if found, otherwise it returns an iterator to DataSet::end().
     *
     * @param datasetPath Path of the DataSet to find.
     *
     * @return a const_iterator pointing to the DataSet if found,
     * DataSet::cend() otherwise.
     */
    const_iterator find(const std::string& datasetPath) const;


    /**
     * @brief Returns an iterator referring to the first DataSet
     * in this DataSet.
     *
     * @return an iterator referring to the first DataSet in this DataSet.
     */
    iterator begin();

    /**
     * @brief Returns an iterator referring to the end of the DataSet.
     * The DataSet pointed to by this iterator is not valid (that is,
     * `end()->valid()` returns `false`).
     *
     * @return an iterator referring to the end of the DataSet.
     */
    iterator end();

    /**
     * @brief Returns a const_iterator referring to the first DataSet
     * in this DataSet.
     *
     * @return a const_iterator referring to the first DataSet in this DataSet.
     */
    const_iterator begin() const;

    /**
     * @brief Returns a const_iterator referring to the end of the DataSet.
     * The DataSet pointed to by this iterator is not valid (that is,
     * `end()->valid()` returns `false`).
     *
     * @return a const_iterator referring to the end of the DataSet.
     */
    const_iterator end() const;

    /**
     * @brief Returns a const_iterator referring to the first DataSet
     * in this DataSet.
     *
     * @return a const_iterator referring to the first DataSet in this DataSet.
     */
    const_iterator cbegin() const;

    /**
     * @brief Returns a const_iterator referring to the end of the DataSet.
     * The DataSet pointed to by this iterator is not valid (that is,
     * `cend()->valid()` return `false`).
     *
     * @return a const_iterator referring to the end of the DataStore.
     */
    const_iterator cend() const;

    /**
     * @brief Returns an iterator pointing to the first DataSet in this
     * DataSet, whose name is not considered to go before lb
     * (i.e., either it is equal or goes after, alphabetically).
     *
     * @param lb DataSet name to search for.
     *
     * @return An iterator to the the first DataSet in this DataSet
     * whose name is not considered to go before lb, or DataStore::end()
     * if all keys are considered to go before it.
     */
    iterator lower_bound(const std::string& lb);

    /**
     * @brief Returns a const_iterator pointing to the first DataSet in this
     * DataSet whose name is not considered to go before lb
     * (i.e., either it is equal or goes after, alphabetically).
     *
     * @param lb DataSet name to search for.
     *
     * @return A const_iterator to the the first DataSet in the DataSet
     * whose name is not considered to go before lb, or DataSet::cend()
     * if all DataSet names are considered to go before it.
     */
    const_iterator lower_bound(const std::string& lb) const;

    /**
     * @brief Returns an iterator pointing to the first DataSet in the
     * DataStore whose key is considered to go after ub.
     *
     * @param ub DataSet name to search for.
     *
     * @return An iterator to the the first DataSet in this DataSet,
     * whose name is considered to go after ub, or DataSet::end() if
     * no DataSet names are considered to go after it.
     */
    iterator upper_bound(const std::string& ub);

    /**
     * @brief Returns a const_iterator pointing to the first DataSet in this
     * DataSet whose key is considered to go after ub.
     *
     * @param ub DataSet name to search for.
     *
     * @return A const_iterator to the the first DataSet in this DataSet
     * whose name is considered to go after ub, or DataSet::end() if
     * no DataSet names are considered to go after it.
     */
    const_iterator upper_bound(const std::string& ub) const;

    /**
     * @brief Returns the RunSet associated with this DataSet.
     *
     * @return the RunSet associated with this DataSet.
     */
    RunSet runs() const;

    /**
     * @brief Accesses an existing run using the []
     * operator. If no run corresponds to the provided run number,
     * the function returns a Run instance d such that
     * r.valid() is false.
     *
     * @param runNumber Number of the run to retrieve.
     *
     * @return a Run corresponding to the provided run number.
     */
    Run operator[](const RunNumber& runNumber) const;

    /**
     * @brief Returns an EventSet pointing to Events in
     * the specified target. If target is -1, the EventSet
     * will also iterate over targets.
     *
     * @param target Target index in which to find events.
     *
     * @return an EventSet associated with the DataSet.
     */
    EventSet events(int target=-1) const;

    /**
     * @brief Return the UUID of the Dataset.
     */
    const UUID& uuid() const;

    /**
     * @brief Builds a DataSet from a known UUID. This can be
     * used e.g. if one process loads/creates a DataSet then gets
     * its UUID via DataSet::uuid() and shares it with other
     * ranks.
     *
     * @param DataStore datastore
     * @param name name of the dataset
     * @param parent full name of the parent dataset
     * @param uuid UUID of the dataset
     *
     * @return Dataset instance.
     */
    static DataSet fromUUID(const DataStore& datastore,
                            std::string name,
                            std::string parent,
                            const UUID& uuid);

    protected:

    /**
     * @see KeyValueContainer::makeProductID
     */
    ProductID makeProductID(const char* label, size_t label_size,
                            const char* type, size_t type_size) const override;

    /**
     * @see KeyValueContainer::storeRawData()
     */
    ProductID storeRawData(DataStore& ds, const ProductID& key, const char* value, size_t vsize) override;

    /**
     * @see KeyValueContainer::storeRawData()
     */
    ProductID storeRawData(WriteBatch& batch, const ProductID& key, const char* value, size_t vsize) override;

    /**
     * @see KeyValueContainer::storeRawData()
     */
    ProductID storeRawData(AsyncEngine& engine, const ProductID& key, const char* value, size_t vsize) override;

    /**
     * @see KeyValueContainer::loadRawData()
     */
    bool loadRawData(const ProductID& key, std::string& value) const override;

    /**
     * @see KeyValueContainer::loadRawData()
     */
    bool loadRawData(const ProductID& key, char* value, size_t* vsize) const override;

    /**
     * @see KeyValueContainer::loadRawData()
     */
    bool loadRawData(const Prefetcher& prefetcher, const ProductID& key, std::string& value) const override;

    /**
     * @see KeyValueContainer::loadRawData()
     */
    bool loadRawData(const Prefetcher& prefetcher, const ProductID& key, char* value, size_t* vsize) const override;

    /**
     * @see KeyValueContainer::loadRawData()
     */
    bool loadRawData(const ProductCache& cache, const ProductID& key, std::string& buffer) const override;

    /**
     * @see KeyValueContainer::loadRawData()
     */
    bool loadRawData(const ProductCache& cache, const ProductID& key, char* value, size_t* vsize) const override;

};

class DataSet::const_iterator {

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

class DataSet::iterator : public DataSet::const_iterator {

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
