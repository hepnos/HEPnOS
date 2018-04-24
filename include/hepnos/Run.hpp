#ifndef __HEPNOS_RUN_H
#define __HEPNOS_RUN_H

#include <memory>
#include <string>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/string.hpp>
#include <hepnos/DataStore.hpp>
#include <hepnos/RunNumber.hpp>
#include <hepnos/SubRun.hpp>
#include <hepnos/Exception.hpp>

namespace hepnos {

class RunSet;

class Run {

    private:

    friend class RunSet;
    friend class DataSet;

    class Impl;
    std::unique_ptr<Impl> m_impl;

    /**
     * @brief Constructor.
     *
     * @param datastore Pointer to the DataStore managing the underlying data.
     * @param level Level of nesting.
     * @param container Full name of the dataset containing the run.
     * @param run Run number.
     */
    Run(DataStore* datastore, uint8_t level, const std::string& container, const RunNumber& run);

    public:

    /**
     * @brief Default constructor. Creates a Run instance such that run.valid() is false.
     */
    Run();

    /**
     * @brief Copy constructor.
     *
     * @param other Run to copy.
     */
    Run(const Run& other);

    /**
     * @brief Move constructor.
     *
     * @param other Run to move.
     */
    Run(Run&& other);

    /**
     * @brief Copy-assignment operator.
     *
     * @param other Run to assign.
     *
     * @return Reference to this Run.
     */
    Run& operator=(const Run& other);

    /**
     * @brief Move-assignment operator.
     *
     * @param other Run to move from.
     *
     * @return Reference to this Run.
     */
    Run& operator=(Run&& other);

    /**
     * @brief Destructor.
     */
    ~Run();

    /**
     * @brief Returns the next Run in the same container,
     * sorted by run number. If no such run exists, a Run instance
     * such that Run::valid() returns false is returned.
     *
     * @return The next Run in the container.
     */
    Run next() const;

    /**
     * @brief Indicates whether this Run instance points to a valid
     * run in the underlying storage service.
     *
     * @return True if the Run instance points to a valid run in the
     * underlying service, false otherwise.
     */
    bool valid() const;

    /**
     * @brief Stores raw key/value data in this Run.
     *
     * @param key Key
     * @param buffer Value
     *
     * @return true if the key did not already exist, false otherwise.
     */
    bool storeRawData(const std::string& key, const std::vector<char>& buffer);

    /**
     * @brief Loads raw key/value data from this Run.
     *
     * @param key Key
     * @param buffer Buffer used to hold the value.
     *
     * @return true if the key exists, false otherwise.
     */
    bool loadRawData(const std::string& key, std::vector<char>& buffer) const;

    /**
     * @brief Compares this Run with another Run. The Runs must point to
     * the same run number within the same container.
     *
     * @param other Run instance to compare against.
     *
     * @return true if the Runs are the same, false otherwise.
     */
    bool operator==(const Run& other) const;

    /**
     * @brief Compares this Run with another Run.
     *
     * @param other Run instance to compare against.
     *
     * @return true if the Runs are different, false otherwise.
     */
    bool operator!=(const Run& other) const;

    /**
     * @brief Returns the run number of this Run. Note that if
     * the Run is not valid, this function will return 0.
     *
     * @return The run number.
     */
    const RunNumber& number() const;

    /**
     * @brief Returns the full name of the DataSet containing
     * this Run.
     *
     * @return the full name of the DataSet containing this Run.
     */
    const std::string& container() const;

    /**
     * @brief Stores a key/value pair into the Run.
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
     * @brief Loads a value associated with a key from the Run.
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

    class const_iterator;
    class iterator;

    /**
     * @brief Searches this Run for an SubRun with 
     * the provided number and returns an iterator to it if found,
     * otherwise it returns an iterator to Run::end().
     *
     * @param srn SubRun number of the SubRun to find.
     *
     * @return an iterator pointing to the SubRun if found,
     * Run::end() otherwise.
     */
    iterator find(const SubRunNumber& srn);

    /**
     * @brief Searches this Run for a SubRun with 
     * the provided number and returns a const_iterator to it 
     * if found, otherwise it returns an iterator to Run::end().
     *
     * @param srn SubRunNumber of the SubRun to find.
     *
     * @return a const_iterator pointing to the SubRun if found,
     * Run::cend() otherwise.
     */
    const_iterator find(const SubRunNumber&) const;

    /**
     * @brief Returns an iterator referring to the first SubRun
     * in this Run.
     *
     * @return an iterator referring to the first SubRun in this Run.
     */
    iterator begin();

    /**
     * @brief Returns an iterator referring to the end of the Run.
     * The SubRun pointed to by this iterator is not valid (that is,
     * `end()->valid()` returns `false`).
     *
     * @return an iterator referring to the end of the Run.
     */
    iterator end();

    /**
     * @brief Returns a const_iterator referring to the first SubRun
     * in this Run.
     *
     * @return a const_iterator referring to the first SubRun in this Run.
     */
    const_iterator begin() const;

    /**
     * @brief Returns a const_iterator referring to the end of the Run.
     * The SubRun pointed to by this iterator is not valid (that is,
     * `end()->valid()` returns `false`).
     *
     * @return a const_iterator referring to the end of the Run.
     */
    const_iterator end() const;

    /**
     * @brief Returns a const_iterator referring to the first SubRun
     * in this Run.
     *
     * @return a const_iterator referring to the first SubRun in this Run.
     */
    const_iterator cbegin() const;

    /**
     * @brief Returns a const_iterator referring to the end of the Run.
     * The SubRun pointed to by this iterator is not valid (that is,
     * `cend()->valid()` return `false`).
     *
     * @return a const_iterator referring to the end of the Run.
     */
    const_iterator cend() const;

    /**
     * @brief Returns an iterator pointing to the first SubRun in this
     * Run, whose SubRunNumber is not lower than lb.
     *
     * @param lb SubRunNumber lower bound to search for.
     *
     * @return An iterator to the first SubRun in this Run
     * whose whose SubRunNumber is not lower than lb, or Run::end() 
     * if all subrun numbers are lower.
     */
    iterator lower_bound(const SubRunNumber&);

    /**
     * @brief Returns a const_iterator pointing to the first SubRun in this
     * Run, whose SubRunNumber is not lower than lb.
     *
     * @param lb SubRunNumber lower bound to search for.
     *
     * @return A const_iterator to the first SubRun in this Run
     * whose whose SubRunNumber is not lower than lb, or Run::cend() 
     * if all subrun numbers are lower.
     */
    const_iterator lower_bound(const SubRunNumber&) const;

    /**
     * @brief Returns an iterator pointing to the first SubRun in the 
     * Run whose SubRunNumber is greater than ub.
     *
     * @param ub SubRunNumber upper bound to search for.
     *
     * @return An iterator to the first SubRun in this Run,
     * whose SubRunNumber is greater than ub, or Run::end() if 
     * no such SubRun exists.
     */
    iterator upper_bound(const SubRunNumber&);

    /**
     * @brief Returns a const_iterator pointing to the first SubRun in the 
     * Run whose SubRunNumber is greater than ub.
     *
     * @param ub SubRunNumber upper bound to search for.
     *
     * @return An const_iterator to the first SubRun in this Run,
     * whose SubRunNumber is greater than ub, or Run::cend() if 
     * no such SubRun exists.
     */
    const_iterator upper_bound(const SubRunNumber&) const;

    /**
     * @brief Accesses an existing ubun using the ()
     * operator. If no run corresponds to the provided subrun number,
     * the function returns a SubRun instance r such that
     * r.valid() is false.
     *
     * @param subRunNumber Number of the subrun to retrieve.
     *
     * @return a SubRun corresponding to the provided subrun number.
     */
    SubRun operator()(const SubRunNumber& subRunNumber) const;

    /**
     * @brief Creates a SubRun within this Run, with the provided
     * SubRunNumber. If a SubRun with the same SubRunNumber already
     * exists, this method does not create a new SubRun but returns
     * a handle to the existing one instead.
     *
     * @param subRunNumber SubRunNumber of the SubRun to create.
     *
     * @return a handle to the created or existing SubRun.
     */
    SubRun createSubRun(const SubRunNumber& subRunNumber);
};

}

#endif
