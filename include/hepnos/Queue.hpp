/*
 * (C) 2022 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_QUEUE_HPP
#define __HEPNOS_QUEUE_HPP

#include <hepnos/QueueAccessMode.hpp>

namespace hepnos {

class QueueImpl;
class DataStore;
class DataStoreImpl;

/**
 * @brief A Queue object is a handle for a queue
 * of items managed in HEPnOS by a QueueProvider.
 */
class Queue {

    friend class DataStore;
    friend class DataStoreImpl;

    public:

    /**
     * @brief Push an item into the queue.
     * The type of the item must match the type declared
     * when opening the queue and the queue must have been
     * opened with PRODUCER mode.
     *
     * @tparam T Type of object
     * @param object Object to push
     */
    template<typename T>
    void push(const T& object);

    /**
     * @brief Check if the queue is currently empty.
     *
     * @return Whether the queue is empty.
     */
    bool empty() const;

    /**
     * @brief Pop an item from the queue.
     * If the queue is empty but some processes have
     * opened it as producer, this function will block
     * until either a new item appears in the queue or
     * all the producers have closed the queue.
     *
     * The function will return true if an object
     * was popped, false otherwise.
     *
     * @tparam T Type of object
     * @param object Object to pop into
     *
     * @return Whether an object was popped.
     */
    template<typename T>
    bool pop(T& object);

    /**
     * @brief Returns the DataStore owning the queue.
     */
    DataStore datastore() const;

    /**
     * @brief Close the queue.
     */
    void close();

    /**
     * @brief Check that the internal state is valid.
     */
    bool valid() const;

    Queue();
    Queue(const Queue&);
    Queue(Queue&&);
    Queue& operator=(const Queue&);
    Queue& operator=(Queue&&);
    ~Queue();

    private:

    Queue(std::shared_ptr<QueueImpl> impl);

    bool popImpl(std::string& data);
    void pushImpl(const std::string& data);
    bool checkTypeImpl(const std::type_info& type_info);

    std::shared_ptr<QueueImpl> m_impl;
};

}

#include <boost/serialization/string.hpp>
#include <hepnos/Demangle.hpp>
#include <hepnos/Exception.hpp>
#include <hepnos/OutputArchive.hpp>
#include <hepnos/InputArchive.hpp>

namespace hepnos {

template<typename T>
void Queue::push(const T& value) {
    if(!checkTypeImpl(typeid(T))) {
        throw Exception("Invalid object type");
    }

    auto value_str = std::string{};

    OutputSizer value_sizer;
    OutputSizeEvaluator value_size_evaluator(value_sizer, 0);
    OutputArchive sizing_oa(value_size_evaluator);
    try {
        sizing_oa << value;
    } catch(const std::exception& e) {
        throw Exception(
            std::string("Exception occured during object size estimation: ") + e.what());
    }

    value_str.reserve(value_sizer.size());

    OutputStringWrapper value_wrapper(value_str);
    OutputStream value_stream(value_wrapper, 0);
    OutputArchive output_oa(value_stream);
    try {
        output_oa << value;
        value_stream.flush();
    } catch(const std::exception& e) {
        throw Exception(
            std::string("Exception occured during serialization: ") + e.what());
    }

    pushImpl(value_str);
}

template<typename T>
bool Queue::pop(T& value) {
    if(!checkTypeImpl(typeid(T))) {
        throw Exception("Invalid object type");
    }
    std::string buffer;
    bool b = popImpl(buffer);
    if(!b) return false;
    try {
        InputStringWrapper value_wrapper(buffer.data(), buffer.size());
        InputStream value_stream(value_wrapper);
        InputArchive ia(datastore(), value_stream);
        ia >> value;
    } catch(const std::exception& e) {
        throw Exception(std::string("Exception occured during serialization: ") + e.what());
    }
    return true;
}

}

#endif
