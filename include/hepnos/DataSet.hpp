#ifndef __HEPNOS_DATA_SET_H
#define __HEPNOS_DATA_SET_H

#include <memory>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/string.hpp>
#include <hepnos/Exception.hpp>
#include <hepnos/RunNumber.hpp>
#include <hepnos/DataStore.hpp>

namespace hepnos {

class RunSet;
class Run;

/**
 * @brief The DataSet class represents a handle to a named dataset
 * stored either at the root of an HEPnOS DataStore service, or within
 * another dataset. It provides functionalities to navigate nested
 * datasets and to load/store data products.
 */
class DataSet {

    friend class DataStore;
    friend class RunSet;

    private:

    /**
     * @brief Constructor.
     *
     * @param ds DataStore to which this DataSet belongs.
     * @param level Level of nesting.
     * @param fullname Full name of the DataSet.
     */
    DataSet(DataStore* ds, uint8_t level, const std::string& fullname);

    /**
     * @brief Constructor.
     *
     * @param ds DataStore to which this DataSet belongs.
     * @param level Level of nesting.
     * @param container Full name of the parent DataSet ("" if no parent).
     * @param name Name of the DataSet.
     */
    DataSet(DataStore* ds, uint8_t level, const std::string& container, const std::string& name);


    /**
     * @brief Implementation class (used for the Pimpl idiom).
     */
    class Impl;

    std::unique_ptr<Impl> m_impl; /*!< Pointer to implementation. */

    public:

    /**
     * @brief Default constructor.
     */
    DataSet();

    /**
     * @brief Copy-constructor.
     *
     * @param other DataSet to copy.
     */
    DataSet(const DataSet& other);

    /**
     * @brief Move-constructor.
     *
     * @param other DataSet to move.
     */
    DataSet(DataSet&& other);

    /**
     * @brief Copy-assignment operator.
     *
     * @param other DataSet to copy.
     *
     * @return this.
     */
    DataSet& operator=(const DataSet& other);

    /**
     * @brief Move-assignment operator.
     *
     * @param other DataSet to move.
     *
     * @return this.
     */
    DataSet& operator=(DataSet&& other);

    /**
     * @brief Destructor.
     */
    ~DataSet();

    /**
     * @brief Name of the DataSet.
     *
     * @return the name of the DataSet.
     */
    const std::string& name() const;

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
     * @brief Stores binary data associated with a particular key into this DataSet.
     * This function will return true if the key did not already exist and the
     * write succeeded. It will return false otherwise.
     *
     * @param key Key.
     * @param buffer Binary data to insert.
     *
     * @return true if the key did not already exist and the write succeeded,
     *      false otherwise.
     */
    bool storeRawData(const std::string& key, const std::vector<char>& buffer);

    /**
     * @brief Loads binary data associated with a particular key from the DataSet.
     * This function will return true if the key exists and the read succeeded.
     * It will return false otherwise.
     * 
     * @param key Key.
     * @param buffer Buffer in which to put the binary data.
     *
     * @return true if the key exists and the read succeeded,
     *      false otherwise.
     */
    bool loadRawData(const std::string& key, std::vector<char>& buffer) const;

    /**
     * @brief Stores a key/value pair into the DataSet.
     * The type of the key should have operator<< available
     * to stream it into a std::stringstream for the purpose
     * of converting it into an std::string. The resulting
     * string must not have the "/" or "%" characters. The
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
    bool store(const K& key, const V& value) {
        std::stringstream ss_value;
        boost::archive::binary_oarchive oa(ss_value);
        try {
            oa << value;
        } catch(...) {
            throw Exception("Exception occured during serialization");
        }
        std::string serialized = ss_value.str();
        std::vector<char> buffer(serialized.begin(), serialized.end());
        return storeRawData(key, buffer);
    }

    /**
     * @brief Loads a value associated with a key from the DataSet.
     * The type of the key should have operator<< available
     * to stream it into a std::stringstream for the purpose
     * of converting it into an std::string. The resulting
     * string must not have the "/" or "%" characters. The
     * type of the value must be serializable using Boost.
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
        std::vector<char> buffer;
        if(!loadRawData(key, buffer)) {
            return false;
        }
        try {
            std::string serialized(buffer.begin(), buffer.end());
            std::stringstream ss(serialized);
            boost::archive::binary_iarchive ia(ss);
            ia >> value;
        } catch(...) {
            throw Exception("Exception occured during serialization");
        }
        return true;
    }

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
     * DataSet. This name must not have the '/' and '#' characters.
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

    typedef DataStore::const_iterator const_iterator;
    typedef DataStore::iterator iterator;

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
     * @brief Searches this DataSet for an DataSet with 
     * the provided name and returns an iterator to it if found,
     * otherwise it returns an iterator to DataStore::end().
     *
     * @param datasetName Name of the DataSet to find.
     *
     * @return an iterator pointing to the DataSet if found,
     * DataSet::end() otherwise.
     */
    iterator find(const std::string& datasetName);

    /**
     * @brief Searches this DataSet for an DataSet with 
     * the provided name and returns a const_iterator to it 
     * if found, otherwise it returns an iterator to DataSet::end().
     *
     * @param datasetName Name of the DataSet to find.
     *
     * @return a const_iterator pointing to the DataSet if found,
     * DataSet::cend() otherwise.
     */
    const_iterator find(const std::string& datasetName) const;


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
     * @brief Returns a reference to the RunSet associated with this DataSet.
     *
     * @return a reference to the RunSet associated with this DataSet.
     */
    RunSet& runs();

    /**
     * @brief Returns a reference to the RunSet associated with this DataSet.
     *
     * @return a reference to the RunSet associated with this DataSet.
     */
    const RunSet& runs() const;

    /**
     * @brief Accesses an existing run using the ()
     * operator. If no run corresponds to the provided run number,
     * the function returns a Run instance d such that
     * r.valid() is false.
     *
     * @param runNumber Number of the run to retrieve.
     *
     * @return a Run corresponding to the provided run number.
     */
    Run operator()(const RunNumber& runNumber) const;
};

}

#endif
