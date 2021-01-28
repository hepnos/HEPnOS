#include "hepnos/AsyncEngine.hpp"
#include "hepnos/DataStore.hpp"
#include "AsyncEngineImpl.hpp"

namespace hepnos {

AsyncEngine::AsyncEngine() {}

AsyncEngine::AsyncEngine(DataStore& ds, size_t num_threads)
: m_impl(std::make_shared<AsyncEngineImpl>(ds.m_impl, num_threads)) {}

void AsyncEngine::wait() {
    if(m_impl)
        m_impl->wait();
}

const std::vector<std::string>& AsyncEngine::errors() const {
    static std::vector<std::string> _default;
    if(m_impl)
        return m_impl->m_errors;
    else
        return _default;
}

std::vector<int> AsyncEngine::getXstreamRanks() const {
    std::vector<int> result;
    if(!m_impl) return result;
    for(auto& es : m_impl->m_xstreams) {
        result.push_back(es->get_rank());
    }
    return result;
}

}
