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

Queue::Queue(std::shared_ptr<QueueImpl> impl)
: m_impl(std::move(impl)) {}

bool Queue::valid() const {
    return m_impl && m_impl->m_datastore;
}

bool Queue::empty() const {
    // TODO
}

DataStore Queue::datastore() const {
    // TODO
}

void Queue::close() {
    // TODO
}

void Queue::popImpl(std::string& data) {
    // TODO
}

void Queue::pushImpl(const std::string& data) {
    // TODO
}

bool Queue::checkTypeImpl(const std::type_info& type_info) {
    // TODO
}

}
