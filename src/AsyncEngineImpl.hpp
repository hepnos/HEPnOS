#ifndef __HEPNOS_ASYNC_ENGINE_IMPL_HPP
#define __HEPNOS_ASYNC_ENGINE_IMPL_HPP

#include <thallium.hpp>
#include "DataStoreImpl.hpp"
#include "hepnos/Exception.hpp"

namespace tl = thallium;

namespace hepnos {

class WriteBatchImpl;

class AsyncEngineImpl {

    friend class WriteBatchImpl;

    std::shared_ptr<DataStoreImpl>        m_datastore;
    tl::pool                              m_pool;
    std::vector<tl::managed<tl::xstream>> m_xstreams;

    public:

    AsyncEngineImpl(const std::shared_ptr<DataStoreImpl>& ds, size_t num_threads)
    : m_datastore(ds) {
        if(num_threads > 0) {
            ABT_pool p = ABT_POOL_NULL;
            int ret = ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_MPMC, ABT_TRUE, &p);
            if(ret != ABT_SUCCESS) {
                throw Exception("Could not create Argobots thread pool");
            }
            m_pool = tl::pool(p);
            for(size_t i=0; i < num_threads; i++) {
                m_xstreams.push_back(tl::xstream::create(tl::scheduler::predef::deflt, m_pool));
            }
            for(auto& es : m_xstreams) {
                es->start();
            }
        } else {
            auto current_es = tl::xstream::self();
            auto pools = current_es.get_main_pools(1);
            if(pools.size() != 1) {
                throw Exception("Could not get current execution stream's main Argobots pool");
            }
        }
    }

    ~AsyncEngineImpl() {
        for(auto& es : m_xstreams) {
            es->join();
        }
    }

};

}

#endif
