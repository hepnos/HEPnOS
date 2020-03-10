/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_RUN_H
#define __HEPNOS_RUN_H

#include <memory>
#include <string>
#include <hepnos/DataStore.hpp>
#include <hepnos/RunNumber.hpp>
#include <hepnos/SubRun.hpp>
#include <hepnos/Exception.hpp>
#include <hepnos/KeyValueContainer.hpp>

namespace hepnos {

constexpr const int RunDescriptorLength = 24;

class Prefetcher;
class RunSet;

struct RunDescriptor {
    char data[RunDescriptorLength];
};

class Run : public KeyValueContainer {

    private:

    friend class SubRun;
    friend class RunSet;
    friend class DataSet;

    std::shared_ptr<ItemImpl> m_impl;

    /**
     * @brief Constructor.
     */
    Run(const std::shared_ptr<ItemImpl>& impl);
    Run(std::shared_ptr<ItemImpl>&& impl);

    public:

    typedef SubRun value_type;

    /**
     * @brief Default constructor. Creates a Run instance such that run.valid() is false.
     */
    Run();

    /**
     * @brief Copy constructor.
     *
     * @param other Run to copy.
     */
    Run(const Run& other) = default;

    /**
     * @brief Move constructor.
     *
     * @param other Run to move.
     */
    Run(Run&& other) = default;

    /**
     * @brief Copy-assignment operator.
     *
     * @param other Run to assign.
     *
     * @return Reference to this Run.
     */
    Run& operator=(const Run& other) = default;

    /**
     * @brief Move-assignment operator.
     *
     * @param other Run to move from.
     *
     * @return Reference to this Run.
     */
    Run& operator=(Run&& other) = default;

    /**
     * @brief Destructor.
     */
    ~Run() = default;

    /**
     * @brief Overrides datastore from KeyValueContainer class.
     */
    DataStore datastore() const override;

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
     * @return a valid ProductID if the key did not already exist, an invalid one otherwise.
     */
    ProductID storeRawData(const std::string& key, const char* value, size_t vsize) override;
    ProductID storeRawData(AsyncEngine& async, const std::string& key, const char* value, size_t vsize) override;
    ProductID storeRawData(WriteBatch& batch, const std::string& key, const char* value, size_t vsize) override;

    /**
     * @brief Loads raw key/value data from this Run.
     *
     * @param key Key
     * @param buffer Buffer used to hold the value.
     *
     * @return true if the key exists, false otherwise.
     */
    bool loadRawData(const std::string& key, std::string& buffer) const override;
    bool loadRawData(const std::string& key, char* value, size_t* vsize) const override;

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
    iterator find(const SubRunNumber& srn, const Prefetcher& prefetcher);

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
    const_iterator find(const SubRunNumber&, const Prefetcher&) const;

    /**
     * @brief Returns an iterator referring to the first SubRun
     * in this Run.
     *
     * @return an iterator referring to the first SubRun in this Run.
     */
    iterator begin();
    iterator begin(const Prefetcher&);

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
    const_iterator begin(const Prefetcher&) const;

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
    const_iterator cbegin(const Prefetcher&) const;

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
    iterator lower_bound(const SubRunNumber&, const Prefetcher&);

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
    const_iterator lower_bound(const SubRunNumber&, const Prefetcher&) const;

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
    iterator upper_bound(const SubRunNumber&, const Prefetcher&);

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
    const_iterator upper_bound(const SubRunNumber&, const Prefetcher&) const;

    /**
     * @brief Accesses an existing ubun using the []
     * operator. If no run corresponds to the provided subrun number,
     * the function returns a SubRun instance r such that
     * r.valid() is false.
     *
     * @param subRunNumber Number of the subrun to retrieve.
     *
     * @return a SubRun corresponding to the provided subrun number.
     */
    SubRun operator[](const SubRunNumber& subRunNumber) const;

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
    SubRun createSubRun(AsyncEngine& async, const SubRunNumber& subRunNumber);
    SubRun createSubRun(WriteBatch& batch, const SubRunNumber& subRunNumber);

    /**
     * @brief Fills a RunDescriptor with the information from this Run object.
     *
     * @param descriptor RunDescriptor to fill.
     */
    void toDescriptor(RunDescriptor& descriptor);

    /**
     * @brief Creates a Run instance from a RunDescriptor.
     * If validate is true, this function will check that the corresponding Run
     * exists in the DataStore. If it does not exist, the returned Run will be
     * invalid. validate can be set to false if, for example, the client
     * application already knows by some other means that the Run exists.
     *
     * @param ds DataStore
     * @param descriptor RunDescriptor
     * @param validate whether to validate the existence of the Run.
     *
     * @return A Run object.
     */
    static Run fromDescriptor(const DataStore& ds, const RunDescriptor& descriptor, bool validate=true);
};

class Run::const_iterator {

    friend class Run;

    protected:

    /**
     * @brief Implementation of the class (using Pimpl idiom)
     */
    class Impl;
    std::unique_ptr<Impl> m_impl; /*!< Pointer to implementation */

    public:

    /**
     * @brief Constructor. Creates a const_iterator pointing
     * to an invalid SubRun.
     */
    const_iterator();

    /**
     * @brief Constructor. Creates a const_iterator pointing
     * to a given SubRun. The SubRun may or may not be valid. 
     *
     * @param current SubRun to make the const_iterator point to.
     */
    const_iterator(const SubRun& current);

    /**
     * @brief Constructor. Creates a const_iterator pointing
     * to a given SubRun. The SubRun may or may not be valid. 
     *
     * @param current SubRun to make the const_iterator point to.
     */
    const_iterator(SubRun&& current);

    typedef const_iterator self_type;
    typedef SubRun value_type;
    typedef SubRun& reference;
    typedef SubRun* pointer;
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
     * to the SubRun this const_iterator points to.
     *
     * @return a const reference to the DataSet this 
     *      const_iterator points to.
     */
    const reference operator*();

    /**
     * @brief Returns a const pointer to the SubRun this
     * const_iterator points to.
     *
     * @return a const pointer to the SubRun this 
     *      const_iterator points to.
     */
    const pointer operator->();

    /**
     * @brief Compares two const_iterators. The two const_iterators
     * are equal if they point to the same SubRun or if both
     * correspond to Run::cend().
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

class Run::iterator : public Run::const_iterator {

    public:

    /**
     * @brief Constructor. Builds an iterator pointing to an
     * invalid SubRun.
     */
    iterator();

    /**
     * @brief Constructor. Builds an iterator pointing to
     * an existing SubRun. The SubRun may or may not be
     * valid.
     *
     * @param current SubRun to point to.
     */
     iterator(const SubRun& current);

    /**
     * @brief Constructor. Builds an iterator pointing to
     * an existing SubRun. The SubRun may or may not be
     * valid.
     *
     * @param current SubRun to point to.
     */
     iterator(SubRun&& current);

     typedef iterator self_type;
     typedef SubRun value_type;
     typedef SubRun& reference;
     typedef SubRun* pointer;
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
      * to the SubRun this iterator points to.
      *
      * @return A reference to the SubRun this iterator
      *      points to.
      */
     reference operator*();

     /**
      * @brief Returns a pointer to the SubRun this iterator
      * points to.
      *
      * @return A pointer to the SubRun this iterator points to.
      */
     pointer operator->();
};

}

#endif
