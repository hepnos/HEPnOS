#ifndef __HEPNOS_DATA_SET_H
#define __HEPNOS_DATA_SET_H

#include <memory>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/string.hpp>
#include <hepnos/Exception.hpp>
#include <hepnos/DataStore.hpp>

namespace hepnos {

/**
 * @brief The DataSet class represents a handle to a named dataset
 * stored either at the root of an HEPnOS DataStore service, or within
 * another dataset. It provides functionalities to navigate nested
 * datasets and to load/store data products.
 */
class DataSet {

    friend class DataStore;

    private:

    /**
     * @brief Constructor.
     *
     * @param ds DataStore to which this DataSet belongs.
     * @param level Level of nesting.
     * @param fullname Full name of the DataSet.
     */
    DataSet(DataStore& ds, uint8_t level, const std::string& fullname);

    /**
     * @brief Constructor.
     *
     * @param ds DataStore to which this DataSet belongs.
     * @param level Level of nesting.
     * @param container Full name of the parent DataSet ("" if no parent).
     * @param name Name of the DataSet.
     */
    DataSet(DataStore& ds, uint8_t level, const std::string& container, const std::string& name);

    /**
     * @brief Stores binary data associated with a particular key into this DataSet.
     * This function will return true if the key did not already exist and the
     * write succeeded. It will return false otherwise.
     *
     * @param key Key.
     * @param buffer Binary data to insert.
     *
     * @return trye if the key did not already exist and the write succeeded,
     *      false otherwise.
     */
    bool storeBuffer(const std::string& key, const std::vector<char>& buffer);

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
    bool loadBuffer(const std::string& key, std::vector<char>& buffer) const;

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
     * @brief Stores a key/value pair into the DataSet.
     * The type of the key should have operator<< available
     * to stream it into a std::stringstream for the purpose
     * of converting it into an std::string. The resulting
     * string must not have the "/" or "#" characters. The
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
        return storeBuffer(key, buffer);
    }

    /**
     * @brief Loads a value associated with a key from the DataSet.
     * The type of the key should have operator<< available
     * to stream it into a std::stringstream for the purpose
     * of converting it into an std::string. The resulting
     * string must not have the "/" or "#" characters. The
     * type of the value must be serializable using Boost.
     *
     * @tparam K type of the key.
     * @tparam V type of the value.
     * @param key Key to load.
     * @param value Value to load.
     *
     * @return bool if the 
     */
    template<typename K, typename V>
    bool load(const K& key, V& value) const {
        std::vector<char> buffer;
        if(!loadBuffer(key, buffer)) {
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
};

}

#endif
