#include "hepnos/AsyncEngine.hpp"
#include "hepnos/DataStore.hpp"
#include "AsyncEngineImpl.hpp"

namespace hepnos {

AsyncEngine::AsyncEngine(DataStore& ds, size_t num_threads)
: m_impl(std::make_shared<AsyncEngineImpl>(ds.m_impl, num_threads)) {}

}
