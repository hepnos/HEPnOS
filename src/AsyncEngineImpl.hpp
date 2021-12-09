#ifndef __HEPNOS_ASYNC_ENGINE_IMPL_HPP
#define __HEPNOS_ASYNC_ENGINE_IMPL_HPP

#include <thallium.hpp>
#include "DataStoreImpl.hpp"
#include "hepnos/Exception.hpp"

namespace tl = thallium;

namespace hepnos {

class WriteBatchImpl;
class AsyncPrefetcherImpl;
class ParallelEventProcessor;

class AsyncEngineImpl {

    friend class WriteBatchImpl;
    friend class AsyncEngine;
    friend class AsyncPrefetcherImpl;
    friend class ParallelEventProcessor;

    public:

    std::shared_ptr<DataStoreImpl>        m_datastore;
    tl::pool                              m_pool;
    std::vector<tl::managed<tl::xstream>> m_xstreams;
    std::vector<std::string>              m_errors;
    tl::mutex                             m_errors_mtx;

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
        } else {
            auto current_es = tl::xstream::self();
            auto pools = current_es.get_main_pools(1);
            if(pools.size() != 1) {
                throw Exception("Could not get current execution stream's main Argobots pool");
            }
            m_pool = pools[0];
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
                db.put(product_id.m_key.data(), product_id.m_key.size(),
                       data.data(), data.size(), YOKAN_MODE_NEW_ONLY);
            } catch(yokan::Exception& ex) {
                std::lock_guard<tl::mutex> lock(m_errors_mtx);
                if(ex.code() == YOKAN_ERR_KEY_EXISTS) {
                    m_errors.push_back(
                            std::string("Product ")
                            +productName
                            +" already exists for item "
                            +id.to_string());
                } else {
                    m_errors.push_back(
                            std::string("yokan::Database::put(): ")
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
        ItemType type = ItemType::RUN;
        if(subrun_number != InvalidSubRunNumber) {
            type = ItemType::SUBRUN;
            if(event_number != InvalidEventNumber)
                type = ItemType::EVENT;
        }
        // make a thread that will store the data
        m_pool.make_thread([this, id, type, ds=m_datastore]() {
            // locate db
            auto& db = ds->locateItemDb(type, id);
            try {
                db.put(&id, sizeof(id), nullptr, 0, YOKAN_MODE_NEW_ONLY);
            } catch(yokan::Exception& ex) {
                if(ex.code() != YOKAN_ERR_KEY_EXISTS) {
                    std::lock_guard<tl::mutex> lock(m_errors_mtx);
                    m_errors.push_back(
                            std::string("yokan::Database::put(): ")
                            +ex.what());
                }
            }
        });

        return true;
    }

    void wait() {
        // join the set of ES
        for(auto& es : m_xstreams) {
            es->join();
        }
        // revive the ES
        for(auto& es : m_xstreams) {
            es->revive();
        }
    }
};

}

#endif
