/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_RUN_SET_H
#define __HEPNOS_RUN_SET_H

#include <memory>
#include <hepnos/Exception.hpp>
#include <hepnos/DataStore.hpp>
#include <hepnos/DataSet.hpp>
#include <hepnos/RunNumber.hpp>
#include <hepnos/Run.hpp>

namespace hepnos {

/**
 * @brief The RunSet class is a helper class to access Runs
 * stored in a particular DataSet.
 */
class RunSet {

    friend class DataSet::Impl;
    friend class DataSet;

    private:

    /**
     * @brief Constructor.
     *
     * @param ds DataSet to which this RunSet belongs.
     */
    RunSet(DataSet* ds);

    /**
     * @brief Implementation class (used for the Pimpl idiom).
     */
    class Impl;

    std::unique_ptr<Impl> m_impl; /*!< Pointer to implementation. */

    public:

    /**
     * @brief Copy-constructor. Deleted.
     *
     * @param other RunSet to copy.
     */
    RunSet(const RunSet& other) = delete;

    /**
     * @brief Move-constructor. Deleted.
     *
     * @param other RunSet to move.
     */
    RunSet(RunSet&& other) = delete;

    /**
     * @brief Copy-assignment operator. Deleted.
     *
     * @param other RunSet to copy.
     *
     * @return this.
     */
    RunSet& operator=(const RunSet& other) = delete;

    /**
     * @brief Move-assignment operator. Deleted.
     *
     * @param other RunSet to move.
     *
     * @return this.
     */
    RunSet& operator=(RunSet&& other) = delete;

    /**
     * @brief Destructor.
     */
    ~RunSet();

    class const_iterator;
    class iterator;

    /**
     * @brief Accesses an existing Run using the ()
     * operator. If no Run correspond to the provided number,
     * the function returns a Run instance r such that
     * r.valid() is false.
     *
     * @param runNumber run number of the Run to retrieve.
     *
     * @return a Run corresponding to the provided number.
     */
    Run operator()(const RunNumber& runNumber);

    /**
     * @brief Searches this RunSet for a Run with 
     * the provided run number and returns an iterator to 
     * it if found, otherwise it returns an iterator pointing 
     * to RunSet::end().
     *
     * @param runNumber Run number of the Run to find.
     *
     * @return an iterator pointing to the Run if found,
     * RunSet::end() otherwise.
     */
    iterator find(const RunNumber& runNumber);

    /**
     * @brief Searches this RunSet for a Run with 
     * the provided run number and returns an const_iterator to 
     * it if found, otherwise it returns an iterator pointing 
     * to RunSet::cend().
     *
     * @param runNumber Run number of the Run to find.
     *
     * @return a const_iterator pointing to the Run if found,
     * RunSet::cend() otherwise.
     */
    const_iterator find(const RunNumber& runNumber) const;


    /**
     * @brief Returns an iterator referring to the first Run
     * in this RunSet.
     *
     * @return an iterator referring to the first Run in this RunSet.
     */
    iterator begin();

    /**
     * @brief Returns an iterator referring to the end of the RunSet.
     * The RunSet pointed to by this iterator is not valid (that is,
     * `end()->valid()` returns `false`).
     *
     * @return an iterator referring to the end of the RunSet.
     */
    iterator end();

    /**
     * @brief Returns a const_iterator referring to the first Run
     * in this RunSet.
     *
     * @return an iterator referring to the first Run in this RunSet.
     */
    const_iterator begin() const;

    /**
     * @brief Returns a const_iterator referring to the end of the RunSet.
     * The RunSet pointed to by this iterator is not valid (that is,
     * `end()->valid()` returns `false`).
     *
     * @return a const_iterator referring to the end of the RunSet.
     */
    const_iterator end() const;

    /**
     * @brief Returns a const_iterator referring to the first Run
     * in this RunSet.
     *
     * @return a const_iterator referring to the first Run in this RunSet.
     */
    const_iterator cbegin() const;

    /**
     * @brief Returns a const_iterator referring to the end of the RunSet.
     * The RunSet pointed to by this iterator is not valid (that is,
     * `cend()->valid()` return `false`).
     *
     * @return a const_iterator referring to the end of the RunSet.
     */
    const_iterator cend() const;

    /**
     * @brief Returns an iterator pointing to the first Run in this
     * RunSet, whose number is equal or greater than lb.
     *
     * @param lb Run number to search for.
     *
     * @return An iterator to the the first Run in this RunSet 
     * whose number is equal or greater than, lb or RunSet::end() 
     * if such a Run does not exist.
     */
    iterator lower_bound(const RunNumber& lb);

    /**
     * @brief Returns a const_iterator pointing to the first Run in this
     * RunSet whose number is equal or greater than lb.
     *
     * @param lb Run number to search for.
     *
     * @return A const_iterator to the the first Run in this RunSet 
     * whose number is equal or greater than, lb or RunSet::end() 
     * if such a Run does not exist.
     */
    const_iterator lower_bound(const RunNumber& lb) const;

    /**
     * @brief Returns an iterator pointing to the first Run in the 
     * RunSet whose number is strictly greater than ub.
     *
     * @param ub Run number to search for.
     *
     * @return An iterator to the the first Run in this RunSet,
     * whose number is stricly greater than ub, or RunSet::end() if 
     * no such a Run exist.
     */
    iterator upper_bound(const RunNumber& ub);

    /**
     * @brief Returns a const_iterator pointing to the first Run in the 
     * RunSet whose number is strictly greater than ub.
     *
     * @param ub Run number to search for.
     *
     * @return A const_iterator to the the first Run in this RunSet,
     * whose number is stricly greater than ub, or RunSet::end() if 
     * no such a Run exist.
     */
    const_iterator upper_bound(const RunNumber& ub) const;
};

class RunSet::const_iterator {

    protected:

    /**
     * @brief Implementation of the class (using Pimpl idiom)
     */
    class Impl;
    std::unique_ptr<Impl> m_impl; /*!< Pointer to implementation */

    public:
    /**
     * @brief Constructor. Creates a const_iterator pointing
     * to an invalid Run.
     */
    const_iterator();

    /**
     * @brief Constructor. Creates a const_iterator pointing
     * to a given Run. The Run may or may not be valid. 
     *
     * @param current Run to make the const_iterator point to.
     */
    const_iterator(const Run& current);

    /**
     * @brief Constructor. Creates a const_iterator pointing
     * to a given Run. The Run may or may not be valid. 
     *
     * @param current Run to make the const_iterator point to.
     */
    const_iterator(Run&& current);

    typedef const_iterator self_type;
    typedef Run value_type;
    typedef Run& reference;
    typedef Run* pointer;
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
     * correspond to RunSet::cend().
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

class RunSet::iterator : public RunSet::const_iterator {

    public:

    /**
     * @brief Constructor. Builds an iterator pointing to an
     * invalid Run.
     */
    iterator();

    /**
     * @brief Constructor. Builds an iterator pointing to
     * an existing Run. The Run may or may not be valid.
     *
     * @param current Run to point to.
     */
    iterator(const Run& current);

    /**
     * @brief Constructor. Builds an iterator pointing to
     * an existing Run. The Run may or may not be valid.
     *
     * @param current DataSet to point to.
     */
    iterator(Run&& current);

    typedef iterator self_type;
    typedef Run value_type;
    typedef Run& reference;
    typedef Run* pointer;
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
