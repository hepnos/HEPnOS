/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_EVENT_SET_H
#define __HEPNOS_EVENT_SET_H

#include <memory>
#include <hepnos/Exception.hpp>
#include <hepnos/DataStore.hpp>
#include <hepnos/EventNumber.hpp>
#include <hepnos/RunNumber.hpp>
#include <hepnos/SubRunNumber.hpp>

namespace hepnos {

class EventSetImpl;

/**
 * @brief The EventSet class is a helper class to access Events
 * stored down in a particular DataSet or Run (bypassing
 * intermediate nesting levels).
 */
class EventSet {

    friend class DataSet;

    private:

    /**
     * @brief Implementation class (used for the Pimpl idiom).
     */
    std::shared_ptr<EventSetImpl> m_impl; /*!< Pointer to implementation. */

    /**
     * @brief Constructor.
     */
    EventSet(const std::shared_ptr<EventSetImpl>& impl);
    EventSet(std::shared_ptr<EventSetImpl>&& impl);

    public:

    typedef Event value_type;

    /**
     * @brief Copy-constructor.
     *
     * @param other EventSet to copy.
     */
    EventSet(const EventSet& other) = default;

    /**
     * @brief Move-constructor. Deleted.
     *
     * @param other EventSet to move.
     */
    EventSet(EventSet&& other) = default;

    /**
     * @brief Copy-assignment operator.
     *
     * @param other EventSet to copy.
     *
     * @return this.
     */
    EventSet& operator=(const EventSet& other) = default;

    /**
     * @brief Move-assignment operator. Deleted.
     *
     * @param other EventSet to move.
     *
     * @return this.
     */
    EventSet& operator=(EventSet&& other) = default;

    /**
     * @brief Destructor.
     */
    ~EventSet() = default;

    class const_iterator;
    class iterator;

    /**
     * @brief Get the DataStore to which this EventSet belongs.
     */
    DataStore datastore() const;

    /**
     * @brief Returns an iterator referring to the first Event
     * in this EventSet.
     *
     * @return an iterator referring to the first Event in this EventSet.
     */
    iterator begin();

    /**
     * @brief Returns an iterator referring to the end of the EventSet.
     * The EventSet pointed to by this iterator is not valid (that is,
     * `end()->valid()` returns `false`).
     *
     * @return an iterator referring to the end of the EventSet.
     */
    iterator end();

    /**
     * @brief Returns a const_iterator referring to the first Event
     * in this EventSet.
     *
     * @return an iterator referring to the first Event in this EventSet.
     */
    const_iterator begin() const;

    /**
     * @brief Returns a const_iterator referring to the end of the EventSet.
     * The EventSet pointed to by this iterator is not valid (that is,
     * `end()->valid()` returns `false`).
     *
     * @return a const_iterator referring to the end of the EventSet.
     */
    const_iterator end() const;

    /**
     * @brief Returns a const_iterator referring to the first Event
     * in this EventSet.
     *
     * @return a const_iterator referring to the first Event in this EventSet.
     */
    const_iterator cbegin() const;

    /**
     * @brief Returns a const_iterator referring to the end of the EventSet.
     * The EventSet pointed to by this iterator is not valid (that is,
     * `cend()->valid()` return `false`).
     *
     * @return a const_iterator referring to the end of the EventSet.
     */
    const_iterator cend() const;

};

class EventSet::const_iterator {

    friend class EventSet;

    protected:

    /**
     * @brief Implementation of the class (using Pimpl idiom)
     */
    class Impl;
    std::unique_ptr<Impl> m_impl; /*!< Pointer to implementation */

    const_iterator(std::unique_ptr<Impl>&& impl);

    public:
    /**
     * @brief Constructor. Creates a const_iterator pointing
     * to an invalid Event.
     */
    const_iterator();

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
     * correspond to EventSet::cend().
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

class EventSet::iterator : public EventSet::const_iterator {

    friend class EventSet;

    private:

    iterator(std::unique_ptr<EventSet::const_iterator::Impl>&& impl);

    public:

    /**
     * @brief Constructor. Builds an iterator pointing to an
     * invalid Event.
     */
    iterator();

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
