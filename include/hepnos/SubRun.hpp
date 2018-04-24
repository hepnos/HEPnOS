#ifndef __HEPNOS_SUBRUN_H
#define __HEPNOS_SUBRUN_H

#include <memory>
#include <string>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/string.hpp>
#include <hepnos/DataStore.hpp>
#include <hepnos/Event.hpp>
#include <hepnos/SubRunNumber.hpp>
#include <hepnos/Exception.hpp>

namespace hepnos {

class SubRun {

    private:

    friend class Run;

    class Impl;
    std::unique_ptr<Impl> m_impl;

    /**
     * @brief Constructor.
     *
     * @param datastore Pointer to the DataStore managing the underlying data.
     * @param level Level of nesting.
     * @param container Full name of the dataset containing the run.
     * @param run SubRun number.
     */
    SubRun(DataStore* datastore, uint8_t level, const std::string& container, const SubRunNumber& run);

    public:

    /**
     * @brief Default constructor. Creates a SubRun instance such that subrun.valid() is false.
     */
    SubRun();

    /**
     * @brief Copy constructor.
     *
     * @param other SubRun to copy.
     */
    SubRun(const SubRun& other);

    /**
     * @brief Move constructor.
     *
     * @param other SubRun to move.
     */
    SubRun(SubRun&& other);

    /**
     * @brief Copy-assignment operator.
     *
     * @param other SubRun to assign.
     *
     * @return Reference to this Run.
     */
    SubRun& operator=(const SubRun& other);

    /**
     * @brief Move-assignment operator.
     *
     * @param other SubRun to move from.
     *
     * @return Reference to this Run.
     */
    SubRun& operator=(SubRun&& other);

    /**
     * @brief Destructor.
     */
    ~SubRun();

    /**
     * @brief Returns the next SubRun in the same container,
     * sorted by subrun number. If no such subrun exists, a SubRun instance
     * such that SubRun::valid() returns false is returned.
     *
     * @return The next SubRun in the container.
     */
    SubRun next() const;

    /**
     * @brief Indicates whether this SubRun instance points to a valid
     * subrun in the underlying storage service.
     *
     * @return True if the SubRun instance points to a valid subrun in the
     * underlying service, false otherwise.
     */
    bool valid() const;

    /**
     * @brief Stores raw key/value data in this SubRun.
     *
     * @param key Key
     * @param buffer Value
     *
     * @return true if the key did not already exist, false otherwise.
     */
    bool storeRawData(const std::string& key, const std::vector<char>& buffer);

    /**
     * @brief Loads raw key/value data from this SubRun.
     *
     * @param key Key
     * @param buffer Buffer used to hold the value.
     *
     * @return true if the key exists, false otherwise.
     */
    bool loadRawData(const std::string& key, std::vector<char>& buffer) const;

    /**
     * @brief Compares this SubRun with another SubRun. The SubRuns must point to
     * the same subrun number within the same container.
     *
     * @param other SubRun instance to compare against.
     *
     * @return true if the SubRuns are the same, false otherwise.
     */
    bool operator==(const SubRun& other) const;

    /**
     * @brief Compares this SubRun with another SubRun.
     *
     * @param other SubRun instance to compare against.
     *
     * @return true if the SubRuns are different, false otherwise.
     */
    bool operator!=(const SubRun& other) const;

    /**
     * @brief Returns the subrun number of this SubRun. Note that if
     * the SubRun is not valid, this function will return 0.
     *
     * @return The subrun number.
     */
    const SubRunNumber& number() const;

    /**
     * @brief Returns the full name of the Run containing
     * this SubRun.
     *
     * @return the full name of the Run containing this SubRun.
     */
    const std::string& container() const;

    /**
     * @brief Stores a key/value pair into the SubRun.
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
     * @brief Loads a value associated with a key from the SubRun.
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
     * @brief Searches this SubRun for an Event with 
     * the provided number and returns an iterator to it if found,
     * otherwise it returns an iterator to Run::end().
     *
     * @param srn SubRun number of the SubRun to find.
     *
     * @return an iterator pointing to the SubRun if found,
     * SubRun::end() otherwise.
     */
    iterator find(const EventNumber& en);

    /**
     * @brief Searches this SubRun for an Event with 
     * the provided number and returns a const_iterator to it 
     * if found, otherwise it returns an iterator to SubRun::end().
     *
     * @param en EventNumber of the Event to find.
     *
     * @return a const_iterator pointing to the Event if found,
     * SubRun::cend() otherwise.
     */
    const_iterator find(const EventNumber& en) const;

    /**
     * @brief Returns an iterator referring to the first Event
     * in this SubRun.
     *
     * @return an iterator referring to the first Event in this SubRun.
     */
    iterator begin();

    /**
     * @brief Returns an iterator referring to the end of the SubRun.
     * The Event pointed to by this iterator is not valid (that is,
     * `end()->valid()` returns `false`).
     *
     * @return an iterator referring to the end of the SubRun.
     */
    iterator end();

    /**
     * @brief Returns a const_iterator referring to the first Event
     * in this SubRun.
     *
     * @return a const_iterator referring to the first Event in this SubRun.
     */
    const_iterator begin() const;

    /**
     * @brief Returns a const_iterator referring to the end of the SubRun.
     * The Event pointed to by this iterator is not valid (that is,
     * `end()->valid()` returns `false`).
     *
     * @return a const_iterator referring to the end of the SubRun.
     */
    const_iterator end() const;

    /**
     * @brief Returns a const_iterator referring to the first Event
     * in this SubRun.
     *
     * @return a const_iterator referring to the first Event in this SubRun.
     */
    const_iterator cbegin() const;

    /**
     * @brief Returns a const_iterator referring to the end of the SubRun.
     * The Event pointed to by this iterator is not valid (that is,
     * `cend()->valid()` return `false`).
     *
     * @return a const_iterator referring to the end of the Run.
     */
    const_iterator cend() const;

    /**
     * @brief Returns an iterator pointing to the first Event in this
     * SubRun, whose EventNumber is not lower than lb.
     *
     * @param lb EventNumber lower bound to search for.
     *
     * @return An iterator to the first Event in this SubRun
     * whose whose EventNumber is not lower than lb, or SubRun::end() 
     * if all event numbers are lower.
     */
    iterator lower_bound(const EventNumber&);

    /**
     * @brief Returns a const_iterator pointing to the first Event in this
     * SubRun, whose EventNumber is not lower than lb.
     *
     * @param lb EventNumber lower bound to search for.
     *
     * @return A const_iterator to the first Event in this SubRun
     * whose whose EventNumber is not lower than lb, or SubRun::cend() 
     * if all event numbers are lower.
     */
    const_iterator lower_bound(const SubRunNumber&) const;

    /**
     * @brief Returns an iterator pointing to the first Event in the 
     * SubRun whose EventNumber is greater than ub.
     *
     * @param ub EventNumber upper bound to search for.
     *
     * @return An iterator to the first Event in this SubRun,
     * whose EventNumber is greater than ub, or SubRun::end() if 
     * no such Event exists.
     */
    iterator upper_bound(const EventNumber&);

    /**
     * @brief Returns a const_iterator pointing to the first Event in the 
     * SubRun whose EventNumber is greater than ub.
     *
     * @param ub EventNumber upper bound to search for.
     *
     * @return An const_iterator to the first Event in this SubRun,
     * whose EventNumber is greater than ub, or SubRun::cend() if 
     * no such Event exists.
     */
    const_iterator upper_bound(const SubRunNumber&) const;

    /**
     * @brief Accesses an existing event using the ()
     * operator. If no run corresponds to the provided run number,
     * the function returns a Run instance d such that
     * r.valid() is false.
     *
     * @param eventNumber Number of the event to retrieve.
     *
     * @return an Event corresponding to the provided event number.
     */
    Event operator()(const EventNumber& eventNumber) const;

    /**
     * @brief Creates an Event within this SubRun, with the provided
     * EventNumber. If an Event with the same EventNumber already
     * exists, this method does not create a new Event but returns
     * a handle to the existing one instead.
     *
     * @param eventNumber EventNumber of the Event to create.
     *
     * @return a handle to the created or existing Event.
     */
    Event createEvent(const EventNumber& eventNumber);
};

}

#endif
