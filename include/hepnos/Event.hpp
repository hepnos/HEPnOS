/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_EVENT_H
#define __HEPNOS_EVENT_H

#include <memory>
#include <string>
#include <hepnos/DataStore.hpp>
#include <hepnos/EventNumber.hpp>
#include <hepnos/Exception.hpp>
#include <hepnos/KeyValueContainer.hpp>

namespace hepnos {

class Event : public KeyValueContainer {

    private:

    friend class SubRun;

    std::shared_ptr<EventImpl> m_impl;

    /**
     * @brief Constructor.
     */
    Event(std::shared_ptr<EventImpl>&& impl);
    Event(const std::shared_ptr<EventImpl>& impl);

    public:

    /**
     * @brief Default constructor. Creates an Event instance such that event.valid() is false.
     */
    Event();

    /**
     * @brief Copy constructor.
     *
     * @param other Event to copy.
     */
    Event(const Event& other) = default;

    /**
     * @brief Move constructor.
     *
     * @param other Event to move.
     */
    Event(Event&& other) = default;

    /**
     * @brief Copy-assignment operator.
     *
     * @param other Run to assign.
     *
     * @return Reference to this Run.
     */
    Event& operator=(const Event& other) = default;

    /**
     * @brief Move-assignment operator.
     *
     * @param other Run to move from.
     *
     * @return Reference to this Run.
     */
    Event& operator=(Event&& other) = default;

    /**
     * @brief Destructor.
     */
    ~Event() = default;

    /**
     * @brief Overrides datastore() from KeyValueContainer class.
     */
    DataStore datastore() const override;

    /**
     * @brief Returns the next Event in the same container,
     * sorted by event number. If no such event exists, an Event instance
     * such that Event::valid() returns false is returned.
     *
     * @return The next Event in the container.
     */
    Event next() const;

    /**
     * @brief Indicates whether this Event instance points to a valid
     * event in the underlying storage service.
     *
     * @return True if the Event instance points to a valid event in the
     * underlying service, false otherwise.
     */
    bool valid() const;

    /**
     * @brief Stores raw key/value data in this Event.
     *
     * @param key Key
     * @param value Value
     *
     * @return a valid ProductID if the key did not already exist, an invalid one otherwise.
     */
    ProductID storeRawData(const std::string& key, const char* value, size_t vsize) override;
    ProductID storeRawData(WriteBatch& batch, const std::string& key, const char* value, size_t vsize) override;

    /**
     * @brief Loads raw key/value data from this Event.
     *
     * @param key Key
     * @param value Buffer used to hold the value.
     *
     * @return true if the key exists, false otherwise.
     */
    bool loadRawData(const std::string& key, std::string& value) const override;
    bool loadRawData(const std::string& key, char* value, size_t* vsize) const override;

    /**
     * @brief Compares this Event with another Event. The Events must point to
     * the same event number within the same container.
     *
     * @param other Event instance to compare against.
     *
     * @return true if the Events are the same, false otherwise.
     */
    bool operator==(const Event& other) const;

    /**
     * @brief Compares this Event with another Event.
     *
     * @param other Event instance to compare against.
     *
     * @return true if the Events are different, false otherwise.
     */
    bool operator!=(const Event& other) const;

    /**
     * @brief Returns the event number of this Event. Note that if
     * the Event is not valid, this function will return 0.
     *
     * @return The event number.
     */
    const EventNumber& number() const;

};

}

#endif
