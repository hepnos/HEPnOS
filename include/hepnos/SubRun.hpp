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

class Prefetcher;

/**
 * @brief SubRuns can contain Events.
 */
class SubRun : public KeyValueContainer {

    private:

    friend class Event;
    friend class Run;

    std::shared_ptr<ItemImpl> m_impl;

    /**
     * @brief Constructor.
     */
    SubRun(std::shared_ptr<ItemImpl>&& impl);
    SubRun(const std::shared_ptr<ItemImpl>& impl);

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
    SubRun(const SubRun& other) = default;

    /**
     * @brief Move constructor.
     *
     * @param other SubRun to move.
     */
    SubRun(SubRun&& other) = default;

    /**
     * @brief Copy-assignment operator.
     *
     * @param other SubRun to assign.
     *
     * @return Reference to this Run.
     */
    SubRun& operator=(const SubRun& other) = default;

    /**
     * @brief Move-assignment operator.
     *
     * @param other SubRun to move from.
     *
     * @return Reference to this Run.
     */
    SubRun& operator=(SubRun&& other) = default;

    /**
     * @brief Destructor.
     */
    ~SubRun() = default;

    /**
     * @brief Overrides datastore from KeyValueContainer class.
     */
    DataStore datastore() const override;

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
     * @see KeyValueContainer::listProducts()
     */
    std::vector<ProductID> listProducts(const std::string& label="") const;

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
    SubRunNumber number() const;

    /**
     * @brief Returns an instance of the enclosing Run.
     */
    Run run() const;

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
     * @brief Same as find() but associates a Prefetcher object
     * to the resulting iterator to prefetch the next items.
     */
    iterator find(const EventNumber& en, const Prefetcher& prefetcher);

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
     * @brief Same as find() but associates a Prefetcher object
     * to the resulting iterator to prefetch the next items.
     */
    const_iterator find(const EventNumber& en, const Prefetcher& prefetcher) const;

    /**
     * @brief Returns an iterator referring to the first Event
     * in this SubRun.
     *
     * @return an iterator referring to the first Event in this SubRun.
     */
    iterator begin();

    /**
     * @brief Same as begin() but associates a Prefetcher object
     * to the resulting iterator to prefetch the next items.
     */
    iterator begin(const Prefetcher& prefetcher);

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
     * @brief Same as begin() but associates a Prefetcher object
     * to the resulting iterator to prefetch the next items.
     */
    const_iterator begin(const Prefetcher& prefetcher) const;

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
     * @brief Same as cbegin() but associates a Prefetcher object
     * to the resulting iterator to prefetch the next items.
     */
    const_iterator cbegin(const Prefetcher& prefetcher) const;

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
     * @brief Same as lower_bound() but associates a Prefetcher object
     * to the resulting iterator to prefetch the next items.
     */
    iterator lower_bound(const EventNumber&, const Prefetcher&);

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
     * @brief Same as lower_bound() but associates a Prefetcher object
     * to the resulting iterator to prefetch the next items.
     */
    const_iterator lower_bound(const SubRunNumber&, const Prefetcher&) const;

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
     * @brief Same as upper_bound() but associates a Prefetcher object
     * to the resulting iterator to prefetch the next items.
     */
    iterator upper_bound(const EventNumber&, const Prefetcher&);

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
     * @brief Same as upper_bound() but associates a Prefetcher object
     * to the resulting iterator to prefetch the next items.
     */
    const_iterator upper_bound(const SubRunNumber&, const Prefetcher&) const;

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

    /**
     * @brief Creates an Event by pushing the creation operation into
     * a WriteBatch. Note that even though the returned Event object is valid,
     * since the actual creation is delayed until the WriteBatch is flushed,
     * there is no guarantee that this operation will ultimately succeed.
     *
     * @param batch WriteBatch in which to append the operation.
     * @param eventNumber Event number.
     *
     * @return a valid Event object.
     */
    Event createEvent(WriteBatch& batch, const EventNumber& eventNumber);

    /**
     * @brief Creates an Event asynchronously.
     * Note that even though the returned Event object is valid, since
     * the actual creation operation will happen in the background,
     * there is no guarantee that this operation will succeed.
     *
     * @param async AsyncEngine to use to make the operation happen in the background.
     * @param eventNumber Event number.
     *
     * @return a valid Event object.
     */
    Event createEvent(AsyncEngine& async, const EventNumber& eventNumber);

    /**
     * @brief Fills a SubRunDescriptor with the information from this SubRun object.
     *
     * @param descriptor SubRunDescriptor to fill.
     */
    void toDescriptor(SubRunDescriptor& descriptor);

    /**
     * @brief Creates a SubRun instance from a SubRunDescriptor.
     * If validate is true, this function will check that the corresponding SubRun
     * exists in the DataStore. If it does not exist, the returned SubRun will be
     * invalid. validate can be set to false if, for example, the client
     * application already knows by some other means that the SubRun exists.
     *
     * @param ds DataStore
     * @param descriptor SubRunDescriptor
     * @param validate whether to validate the existence of the SubRun.
     *
     * @return A SubRun object.
     */
    static SubRun fromDescriptor(const DataStore& ds, const SubRunDescriptor& descriptor, bool validate=true);

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
    ProductID storeRawData(AsyncEngine& async, const ProductID& key, const char* value, size_t vsize) override;

    /**
     * @brief KeyValueContainer::loadRawData()
     */
    bool loadRawData(const ProductID& key, std::string& buffer) const override;

    /**
     * @brief KeyValueContainer::loadRawData()
     */
    bool loadRawData(const ProductID& key, char* value, size_t* vsize) const override;

    /**
     * @brief KeyValueContainer::loadRawData()
     */
    bool loadRawData(const Prefetcher& prefetcher, const ProductID& key, std::string& buffer) const override;

    /**
     * @brief KeyValueContainer::loadRawData()
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

class SubRun::const_iterator {

    friend class SubRun;

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

namespace boost {
namespace serialization {

    template<typename Archive>
    inline void serialize(Archive& ar, hepnos::SubRunDescriptor& desc, const unsigned int version) {
        ar & boost::serialization::make_binary_object(
                static_cast<void*>(desc.data), hepnos::SubRunDescriptorLength);
    }

}
}

#endif
