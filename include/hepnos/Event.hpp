/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_EVENT_H
#define __HEPNOS_EVENT_H

#include <memory>
#include <string>
#include <cereal/cereal.hpp>
#include <hepnos/DataStore.hpp>
#include <hepnos/EventNumber.hpp>
#include <hepnos/Exception.hpp>
#include <hepnos/KeyValueContainer.hpp>

namespace hepnos {

class SubRun;
class EventSet;

/**
 * @brief The Event class is a KeyValueContainer contained in SubRuns
 * and with no sub-container.
 */
class Event : public KeyValueContainer {

    private:

    friend class SubRun;
    friend class EventSet;

    std::shared_ptr<ItemImpl> m_impl;

    /**
     * @brief Constructor.
     */
    Event(std::shared_ptr<ItemImpl>&& impl);
    Event(const std::shared_ptr<ItemImpl>& impl);

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
     * @see KeyValueContainer::listProducts()
     */
    std::vector<ProductID> listProducts(const std::string& label="") const;

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
    EventNumber number() const;

    /**
     * @brief Returns an intance of the enclosing SubRun.
     *
     * @return Parent SubRun.
     */
    SubRun subrun() const;

    /**
     * @brief Fills an EventDescriptor with the information from this Event object.
     *
     * @param descriptor EventDescriptor to fill.
     */
    void toDescriptor(EventDescriptor& descriptor);

    /**
     * @brief Creates a Event instance from an EventDescriptor.
     * If validate is true, this function will check that the corresponding Event
     * exists in the DataStore. If it does not exist, the returned Event will be
     * invalid. validate can be set to false if, for example, the client
     * application already knows by some other means that the Event exists.
     *
     * @param ds DataStore
     * @param descriptor EventDescriptor
     * @param validate whether to validate the existence of the Event.
     *
     * @return An Event object.
     */
    static Event fromDescriptor(const DataStore& ds, const EventDescriptor& descriptor, bool validate=true);

    protected:

    /**
     * @see KeyValueContainer::makeProductID
     */
    ProductID makeProductID(const char* label, size_t label_size,
                            const char* type, size_t type_size) const override;

};

}

namespace boost {
namespace serialization {

    template<typename Archive>
    inline void serialize(Archive& ar, hepnos::EventDescriptor& desc, const unsigned int version) {
        ar & boost::serialization::make_binary_object(
                static_cast<void*>(desc.data), hepnos::EventDescriptorLength);
    }

}
}
#endif
