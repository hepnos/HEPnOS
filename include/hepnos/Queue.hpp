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

class Queue {

    friend class DataStore;
    friend class DataStoreImpl;

    public:

    template<typename T>
    void push(const T& object);

    bool empty() const;

    template<typename T>
    void pop(T& object);

    DataStore datastore() const;

    void close();

    bool valid() const;

    private:

    Queue(std::shared_ptr<QueueImpl> impl);

    void popImpl(std::string& data);
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
    OutputSizeEvaluator value_size_evaluator(value_sizer);
    OutputArchive sizing_oa(value_size_evaluator);
    try {
        sizing_oa << value;
    } catch(const std::exception& e) {
        throw Exception(
            std::string("Exception occured during object size estimation: ") + e.what());
    }

    value_str.reserve(value_sizer.size());

    OutputStringWrapper value_wrapper(value_str);
    OutputStream value_stream(value_wrapper);
    OutputArchive output_oa(value_stream);
    try {
        output_oa << value;
    } catch(const std::exception& e) {
        throw Exception(
            std::string("Exception occured during serialization: ") + e.what());
    }

    pushImpl(value_str);
}

template<typename T>
void Queue::pop(T& value) {
    if(!checkTypeImpl(typeid(T))) {
        throw Exception("Invalid object type");
    }
    std::string buffer;
    popImpl(buffer);
    try {
        InputStringWrapper value_wrapper(buffer.data(), buffer.size());
        InputStream value_stream(value_wrapper);
        InputArchive ia(datastore(), value_stream);
        ia >> value;
    } catch(const std::exception& e) {
        throw Exception(std::string("Exception occured during serialization: ") + e.what());
    }
}

}

#endif
