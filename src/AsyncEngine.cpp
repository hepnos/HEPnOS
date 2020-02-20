#include "hepnos/AsyncEngine.hpp"
#include "hepnos/DataStore.hpp"
#include "AsyncEngineImpl.hpp"

namespace hepnos {

AsyncEngine::AsyncEngine(DataStore& ds, size_t num_threads)
: m_impl(std::make_shared<AsyncEngineImpl>(ds.m_impl, num_threads)) {}

void AsyncEngine::wait() {
    m_impl->wait();
}

const std::vector<std::string>& AsyncEngine::errors() const {
    return m_impl->m_errors;
}

}
