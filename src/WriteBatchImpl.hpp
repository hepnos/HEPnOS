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

    public:

    struct keyvals {
        std::vector<std::string> m_keys;
        std::vector<std::string> m_values;
    };

    typedef std::unordered_map<const sdskv::database*, keyvals> entries_type;

    WriteBatchStatistics                  m_stats;
    bool                                  m_stats_enabled;
    mutable tl::mutex                     m_stats_mtx;

    std::shared_ptr<DataStoreImpl>        m_datastore;
    std::shared_ptr<AsyncEngineImpl>      m_async_engine;
    entries_type                          m_entries;
    unsigned                              m_max_batch_size;
    tl::condition_variable                m_cond;
    tl::mutex                             m_mutex;
    std::vector<tl::managed<tl::thread>>  m_async_thread;
    bool                                  m_async_thread_should_stop = false;

    void update_keyval_statistics(size_t ksize, size_t vsize) {
        if(!m_stats_enabled) return;
        m_stats.key_sizes.updateWith(ksize);
        if(vsize)
            m_stats.value_sizes.updateWith(vsize);
    }

    void update_operation_statistics(size_t batch_size) {
        if(!m_stats_enabled) return;
        m_stats.batch_sizes.updateWith(batch_size);
    }

    static void writer_thread(WriteBatchImpl& wb,
                              unsigned max_batch_size,
                              const sdskv::database* db, 
                              const std::vector<std::string>& keys,
                              const std::vector<std::string>& vals,
                              Exception* exception,
                              char* ok) {
        *ok = 1;
        std::vector<const void*> batch_keys(max_batch_size);
        std::vector<size_t> batch_keys_sizes(max_batch_size);
        std::vector<const void*> batch_vals(max_batch_size);
        std::vector<size_t> batch_vals_sizes(max_batch_size);
        size_t remaining = keys.size();
        while(remaining != 0) {
            unsigned j = keys.size()-remaining;
            unsigned this_batch_size = std::min<unsigned>(max_batch_size, remaining);
            for(unsigned i=0; i < this_batch_size; i++, j++) {
                batch_keys[i]       = keys[j].data();
                batch_keys_sizes[i] = keys[j].size();
                batch_vals[i]       = vals[j].data();
                batch_vals_sizes[i] = vals[j].size();
            }
            batch_keys.resize(this_batch_size);
            batch_keys_sizes.resize(this_batch_size);
            batch_vals.resize(this_batch_size);
            batch_vals_sizes.resize(this_batch_size);
            try {
                db->put_multi(batch_keys, batch_keys_sizes, batch_vals, batch_vals_sizes);
                wb.update_operation_statistics(this_batch_size);
            } catch(sdskv::exception& ex) {
                if(ex.error() != SDSKV_ERR_KEYEXISTS) {
                    *ok = 0;
                    *exception = Exception(std::string("SDSKV error: ")+ex.what());
                }
            }
            remaining -= this_batch_size;
        }
    }

    static void spawn_writer_threads(WriteBatchImpl& wb, unsigned max_batch_size, entries_type& entries, tl::pool& pool) {
        auto num_threads = entries.size();
        std::vector<tl::managed<tl::thread>> threads;
        std::vector<Exception> exceptions(num_threads);
        std::vector<char>      oks(num_threads);
        unsigned i=0;
        for(auto& e : entries) {
            char* ok = &oks[i];
            Exception* ex = &exceptions[i];
            threads.push_back(pool.make_thread([&wb, max_batch_size, &e, ok, ex]() {
                    writer_thread(wb, max_batch_size, e.first, e.second.m_keys, e.second.m_values, ex, ok);
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
            spawn_writer_threads(batch, batch.m_max_batch_size, entries, batch.m_async_engine->m_pool);
        }
    }

    public:

    WriteBatchImpl(const std::shared_ptr<DataStoreImpl>& ds,
                   unsigned max_batch_size,
                   const std::shared_ptr<AsyncEngineImpl>& async = nullptr)
    : m_datastore(ds)
    , m_async_engine(async)
    , m_max_batch_size(max_batch_size) {
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
            update_keyval_statistics(product_id.m_key.size(), vsize);
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
            update_keyval_statistics(sizeof(id), 0);
        } 
        if(was_empty) {
            m_cond.notify_one();
        }
        return true;
    }

    void flush() {
        if(!m_async_engine) { // flush everything here
            tl::xstream es = tl::xstream::self();
            spawn_writer_threads(*this, m_max_batch_size, m_entries, es.get_main_pools(1)[0]);
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

    void collectStatistics(WriteBatchStatistics& stats) const {
        std::unique_lock<tl::mutex> lock(m_stats_mtx);
        stats = m_stats;
    }
};

}

#endif
