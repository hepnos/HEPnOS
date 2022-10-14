/*
 * (C) 2022 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <memory>
#include <string>
#include <thallium.hpp>
#include "hepnos/DataStore.hpp"
#include "hepnos/Queue.hpp"
#include "QueueImpl.hpp"
#include "DataStoreImpl.hpp"

namespace hepnos {

namespace tl = thallium;

Queue::Queue() = default;
Queue::Queue(const Queue&) = default;
Queue::Queue(Queue&&) = default;
Queue& Queue::operator=(const Queue&) = default;
Queue& Queue::operator=(Queue&&) = default;
Queue::~Queue() = default;

Queue::Queue(std::shared_ptr<QueueImpl> impl)
: m_impl(std::move(impl)) {}

bool Queue::valid() const {
    return m_impl && m_impl->m_datastore;
}

bool Queue::empty() const {
    if(!valid()) {
        throw Exception("Calling Queue member function on an invalid Queue object");
    }
    const auto& full_name = m_impl->m_full_name;
    const auto& provider_handle = m_impl->m_provider_handle;
    auto& rpc = m_impl->m_datastore->m_queue_empty_rpc;
    std::pair<bool, std::string> result;
    try {
        result = static_cast<decltype(result)>(rpc.on(provider_handle)(full_name));
    } catch(std::exception& e) {
        throw Exception(e.what());
    }
    if(!result.second.empty())
        throw Exception(result.second);
    return result.first;
}

DataStore Queue::datastore() const {
    if(!valid()) {
        throw Exception("Calling Queue member function on an invalid Queue object");
    }
    return DataStore(m_impl->m_datastore);
}

void Queue::close() {
    if(!valid()) {
        throw Exception("Calling Queue member function on an invalid Queue object");
    }
    if(m_impl->m_mode == QueueAccessMode::CONSUMER)
        return;
    const auto& full_name = m_impl->m_full_name;
    const auto& provider_handle = m_impl->m_provider_handle;
    auto& rpc = m_impl->m_datastore->m_queue_close_rpc;
    std::pair<bool, std::string> result;
    try {
        result = static_cast<decltype(result)>(rpc.on(provider_handle)(full_name, true));
    } catch(std::exception& e) {
        throw Exception(e.what());
    }
    if(!result.first)
        throw Exception(result.second);
}

bool Queue::popImpl(std::string& data) {
    if(!valid()) {
        throw Exception("Calling Queue member function on an invalid Queue object");
    }
    const auto& full_name = m_impl->m_full_name;
    const auto& provider_handle = m_impl->m_provider_handle;
    auto& rpc = m_impl->m_datastore->m_queue_pop_rpc;
    std::pair<bool, std::string> result;
    try {
        result = static_cast<decltype(result)>(rpc.on(provider_handle)(full_name));
    } catch(std::exception& e) {
        throw Exception(e.what());
    }
    if(!result.first) {
        if(result.second.empty())
            return false;
        else
            throw Exception(result.second);
    }
    data = std::move(result.second);
    return true;
}

void Queue::pushImpl(const std::string& data) {
    if(!valid()) {
        throw Exception("Calling Queue member function on an invalid Queue object");
    }
    if(m_impl->m_mode != QueueAccessMode::PRODUCER)
        throw Exception("Queue was open in consumer mode");
    const auto& full_name = m_impl->m_full_name;
    const auto& provider_handle = m_impl->m_provider_handle;
    auto& rpc = m_impl->m_datastore->m_queue_push_rpc;
    std::pair<bool, std::string> result;
    try {
        result = static_cast<decltype(result)>(rpc.on(provider_handle)(full_name, data));
    } catch(std::exception& e) {
        throw Exception(e.what());
    }
    if(!result.first)
        throw Exception(result.second);
}

bool Queue::checkTypeImpl(const std::type_info& type_info) {
    if(!valid()) {
        throw Exception("Calling Queue member function on an invalid Queue object");
    }
    return type_info == m_impl->m_type_info.get();
}

}
