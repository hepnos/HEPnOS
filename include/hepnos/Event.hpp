#ifndef __HEPNOS_EVENT_H
#define __HEPNOS_EVENT_H

#include <memory>
#include <string>
#include <hepnos/KeyValueContainer.hpp>
#include <hepnos/DataStore.hpp>
#include <hepnos/EventNumber.hpp>
#include <hepnos/Exception.hpp>

namespace hepnos {

class Event : public KeyValueContainer {

    private:

    friend class SubRun;

    class Impl;
    std::unique_ptr<Impl> m_impl;

    /**
     * @brief Constructor.
     *
     * @param datastore Pointer to the DataStore managing the underlying data.
     * @param level Level of nesting.
     * @param container Full name of the container containing the event.
     * @param n Event number.
     */
    Event(DataStore* datastore, uint8_t level, const std::string& container, const EventNumber& n);

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
    Event(const Event& other);

    /**
     * @brief Move constructor.
     *
     * @param other Event to move.
     */
    Event(Event&& other);

    /**
     * @brief Copy-assignment operator.
     *
     * @param other Run to assign.
     *
     * @return Reference to this Run.
     */
    Event& operator=(const Event& other);

    /**
     * @brief Move-assignment operator.
     *
     * @param other Run to move from.
     *
     * @return Reference to this Run.
     */
    Event& operator=(Event&& other);

    /**
     * @brief Destructor.
     */
    ~Event();

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
     * @param buffer Value
     *
     * @return true if the key did not already exist, false otherwise.
     */
    bool storeRawData(const std::string& key, const std::vector<char>& buffer);

    /**
     * @brief Loads raw key/value data from this Event.
     *
     * @param key Key
     * @param buffer Buffer used to hold the value.
     *
     * @return true if the key exists, false otherwise.
     */
    bool loadRawData(const std::string& key, std::vector<char>& buffer) const;

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

    /**
     * @brief Returns the full name of the SubRun containing
     * this Event.
     *
     * @return the full name of the SubRun containing this Event.
     */
    const std::string& container() const;

};

}

#endif
