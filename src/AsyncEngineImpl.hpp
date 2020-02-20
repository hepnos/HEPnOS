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
    friend class AsyncEngine;

    std::shared_ptr<DataStoreImpl>        m_datastore;
    tl::pool                              m_pool;
    std::vector<tl::managed<tl::xstream>> m_xstreams;
    std::vector<std::string>              m_errors;
    tl::mutex                             m_errors_mtx;

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

    ProductID storeRawProduct(const ItemDescriptor& id,
                              const std::string& productName,
                              const char* value, size_t vsize)
    {
        // make a thread that will store the data
        auto product_id = m_datastore->buildProductID(id, productName);
        m_pool.make_thread([this,
                            product_id, id, productName,// passed by copy 
                            ds=m_datastore, // shared pointer
                            data=std::string(value,vsize)]() { // create new string
            auto& db = ds->locateProductDb(product_id);
            try {
                db.put(product_id.m_key, data);
            } catch(sdskv::exception& ex) {
                std::lock_guard<tl::mutex> lock(m_errors_mtx);
                if(ex.error() == SDSKV_ERR_KEYEXISTS) {
                    m_errors.push_back(
                            std::string("Product ")
                            +productName
                            +" already exists for item "
                            +id.to_string());
                } else {
                    m_errors.push_back(
                            std::string("SDSKV error: ")
                            +ex.what());
                }
            }
        });
        return product_id;
    }

    bool createItem(const UUID& containerUUID,
                    const RunNumber& run_number,
                    const SubRunNumber& subrun_number = InvalidSubRunNumber,
                    const EventNumber& event_number = InvalidEventNumber)
    {
        // build the key
        ItemDescriptor id;
        id.dataset = containerUUID;
        id.run     = run_number;
        id.subrun  = subrun_number;
        id.event   = event_number;
        // make a thread that will store the data
        m_pool.make_thread([this, id, ds=m_datastore]() {
            // locate db
            auto& db = ds->locateItemDb(id);
            try {
                db.put(&id, sizeof(id), nullptr, 0);
            } catch(sdskv::exception& ex) {
                if(!ex.error() == SDSKV_ERR_KEYEXISTS) {
                    std::lock_guard<tl::mutex> lock(m_errors_mtx);
                    m_errors.push_back(
                            std::string("SDSKV error: ")
                            +ex.what());
                }
            }
        });

        return true;
    }

    void wait() {
        // join the current set of ES
        for(auto& es : m_xstreams) {
            es->join();
        }
        // create a new set of ES
        std::vector<tl::managed<tl::xstream>> new_es;
        for(unsigned i=0; i < m_xstreams.size(); i++) {
            new_es.push_back(tl::xstream::create(tl::scheduler::predef::deflt, m_pool));
        }
        // replace old es
        m_xstreams = std::move(new_es);
        // starting new ES
        for(auto& es : m_xstreams) es->start();
    }
};

}

#endif
