/*
 * (C) 2019 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRIVATE_WRITEBATCH_IMPL_H
#define __HEPNOS_PRIVATE_WRITEBATCH_IMPL_H

#include <mutex> // for std::lock_guard and std::unique_lock
#include <iostream>
#include <unordered_map>
#include <string>
#include <vector>
#include <thallium.hpp>
#include "hepnos/WriteBatch.hpp"
#include "DataStoreImpl.hpp"
#include "AsyncEngineImpl.hpp"

namespace tl = thallium;

namespace hepnos {

class WriteBatchImpl {

    struct keyvals {
        std::vector<std::string> m_keys;
        std::vector<std::string> m_values;
    };

    typedef std::unordered_map<const sdskv::database*, keyvals> entries_type;

    std::shared_ptr<DataStoreImpl>       m_datastore;
    std::shared_ptr<AsyncEngineImpl>     m_async_engine;
    entries_type                         m_entries;
    tl::condition_variable               m_cond;
    tl::mutex                            m_mutex;
    std::vector<tl::managed<tl::thread>> m_async_thread;
    bool                                 m_async_thread_should_stop = false;

    static void writer_thread(const sdskv::database* db, 
                              const std::vector<std::string>& keys,
                              const std::vector<std::string>& vals,
                              Exception* exception,
                              char* ok) {
        *ok = 1;
        try {
            db->put_multi(keys, vals);
        } catch(sdskv::exception& ex) {
            if(ex.error() != SDSKV_ERR_KEYEXISTS) {
                *ok = 0;
                *exception = Exception(std::string("SDSKV error: ")+ex.what());
            }
        }
    }

    static void spawn_writer_threads(entries_type& entries, tl::pool& pool) {
        auto num_threads = entries.size();
        std::vector<tl::managed<tl::thread>> threads;
        std::vector<Exception> exceptions(num_threads);
        std::vector<char>      oks(num_threads);
        unsigned i=0;
        for(auto& e : entries) {
            char* ok = &oks[i];
            Exception* ex = &exceptions[i];
            threads.push_back(pool.make_thread([&e, ok, ex]() {
                    writer_thread(e.first, e.second.m_keys, e.second.m_values, ex, ok);
            }));
            i += 1;
        }
        for(auto& t : threads) {
            t->join();
        }
        for(unsigned i=0; i < num_threads; i++) {
            if(not oks[i]) throw exceptions[i];
        }
        entries.clear();
    }

    static void async_writer_thread(WriteBatchImpl& batch) {
        while(!(batch.m_async_thread_should_stop && batch.m_entries.empty())) {
            std::unique_lock<tl::mutex> lock(batch.m_mutex);
            while(batch.m_entries.empty()) {
                batch.m_cond.wait(lock);
            }
            if(batch.m_entries.empty())
                continue;
            auto entries = std::move(batch.m_entries);
            batch.m_entries.clear();
            lock.unlock();
            spawn_writer_threads(entries, batch.m_async_engine->m_pool);
        }
    }

    public:

    WriteBatchImpl(const std::shared_ptr<DataStoreImpl>& ds,
                   const std::shared_ptr<AsyncEngineImpl>& async = nullptr)
    : m_datastore(ds)
    , m_async_engine(async) {
        if(m_async_engine) {
            m_async_thread.push_back(
                    m_async_engine->m_pool.make_thread([batch=this](){
                        async_writer_thread(*batch);       
                    })
                );
        }
    }

    ProductID storeRawProduct(const ItemDescriptor& id,
                              const std::string& productName,
                              const char* value, size_t vsize)
    {
        // build the key
        auto product_id = m_datastore->buildProductID(id, productName);
        // locate db
        auto& db = m_datastore->locateProductDb(product_id);
        // insert in the map of entries
        bool was_empty;
        { 
            std::lock_guard<tl::mutex> g(m_mutex);
            was_empty = m_entries.empty();
            keyvals& entry = m_entries[&db];
            entry.m_keys.push_back(product_id.m_key);
            entry.m_values.emplace_back(value, vsize);
        }
        if(was_empty) {
            m_cond.notify_one();
        }
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
        // locate db
        auto& db = m_datastore->locateItemDb(type, id);
        // insert in the map of entries
        bool was_empty;
        {
            std::lock_guard<tl::mutex> lock(m_mutex);
            was_empty = m_entries.empty();
            auto& entry = m_entries[&db];
            entry.m_keys.push_back(std::string(reinterpret_cast<char*>(&id), sizeof(id)));
            entry.m_values.emplace_back();
        } 
        if(was_empty) {
            m_cond.notify_one();
        }
        return true;
    }

    void flush() {
        if(!m_async_engine) { // flush everything here
            tl::xstream es = tl::xstream::self();
            spawn_writer_threads(m_entries, es.get_main_pools(1)[0]);
        } else { // wait for AsyncEngine to have flushed everything
            {
                std::lock_guard<tl::mutex> lock(m_mutex);
                m_async_thread_should_stop = true;
            }
            m_cond.notify_one();
            m_async_thread[0]->join();
        }
    }

    ~WriteBatchImpl() {
        flush();
    }
};

}

#endif
