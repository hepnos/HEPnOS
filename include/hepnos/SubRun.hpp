/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_SUBRUN_H
#define __HEPNOS_SUBRUN_H

#include <memory>
#include <string>
#include <hepnos/DataStore.hpp>
#include <hepnos/Event.hpp>
#include <hepnos/SubRunNumber.hpp>
#include <hepnos/Exception.hpp>
#include <hepnos/KeyValueContainer.hpp>

namespace hepnos {

class SubRun : public KeyValueContainer {

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

    typedef Event value_type;

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
     * @brief Overrides getDataStore from KeyValueContainer class.
     */
    DataStore* getDataStore() const override;

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
     * @return a valid ProductID if the key did not already exist, an invalid one otherwise.
     */
    ProductID storeRawData(const std::string& key, const std::string& value) override;
    ProductID storeRawData(std::string&& key, std::string&& value) override;
    ProductID storeRawData(WriteBatch& batch, const std::string& key, const std::string& value) override;
    ProductID storeRawData(WriteBatch& batch, std::string&& key, std::string&& value) override;

    /**
     * @brief Loads raw key/value data from this SubRun.
     *
     * @param key Key
     * @param buffer Buffer used to hold the value.
     *
     * @return true if the key exists, false otherwise.
     */
    bool loadRawData(const std::string& key, std::string& buffer) const override;

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
     * @brief Accesses an existing event using the []
     * operator. If no run corresponds to the provided run number,
     * the function returns a Run instance d such that
     * r.valid() is false.
     *
     * @param eventNumber Number of the event to retrieve.
     *
     * @return an Event corresponding to the provided event number.
     */
    Event operator[](const EventNumber& eventNumber) const;

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

class SubRun::const_iterator {

    protected:

    /**
     * @brief Implementation of the class (using Pimpl idiom)
     */
    class Impl;
    std::unique_ptr<Impl> m_impl; /*!< Pointer to implementation */

    public:

    /**
     * @brief Constructor. Creates a const_iterator pointing
     * to an invalid Event.
     */
    const_iterator();

    /**
     * @brief Constructor. Creates a const_iterator pointing
     * to a given Event. The Event may or may not be valid. 
     *
     * @param current Event to make the const_iterator point to.
     */
    const_iterator(const Event& current);

    /**
     * @brief Constructor. Creates a const_iterator pointing
     * to a given Event. The Event may or may not be valid. 
     *
     * @param current Event to make the const_iterator point to.
     */
    const_iterator(Event&& current);

    typedef const_iterator self_type;
    typedef Event value_type;
    typedef Event& reference;
    typedef Event* pointer;
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
     * to the Event this const_iterator points to.
     *
     * @return a const reference to the DataSet this 
     *      const_iterator points to.
     */
    const reference operator*();

    /**
     * @brief Returns a const pointer to the Event this
     * const_iterator points to.
     *
     * @return a const pointer to the Event this 
     *      const_iterator points to.
     */
    const pointer operator->();

    /**
     * @brief Compares two const_iterators. The two const_iterators
     * are equal if they point to the same Event or if both
     * correspond to SubRun::cend().
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

class SubRun::iterator : public SubRun::const_iterator {

    public:

    /**
     * @brief Constructor. Builds an iterator pointing to an
     * invalid Event.
     */
    iterator();

    /**
     * @brief Constructor. Builds an iterator pointing to
     * an existing Event. The Event may or may not be
     * valid.
     *
     * @param current Event to point to.
     */
     iterator(const Event& current);

    /**
     * @brief Constructor. Builds an iterator pointing to
     * an existing Event. The Event may or may not be
     * valid.
     *
     * @param current Event to point to.
     */
     iterator(Event&& current);

     typedef iterator self_type;
     typedef Event value_type;
     typedef Event& reference;
     typedef Event* pointer;
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
      * to the Event this iterator points to.
      *
      * @return A reference to the Event this iterator
      *      points to.
      */
     reference operator*();

     /**
      * @brief Returns a pointer to the Event this iterator
      * points to.
      *
      * @return A pointer to the Event this iterator points to.
      */
     pointer operator->();
};

}

#endif
