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
#include <queue>
#include <string>
#include <vector>
#include <thallium.hpp>
#include <spdlog/spdlog.h>
#include "hepnos/WriteBatch.hpp"
#include "DataStoreImpl.hpp"
#include "AsyncEngineImpl.hpp"

namespace tl = thallium;

namespace hepnos {

class WriteBatchImpl {

    public:

    struct keyvals {
        size_t                 m_size = 0;
        std::string            m_packed_keys;
        std::vector<hg_size_t> m_packed_key_sizes;
        std::string            m_packed_vals;
        std::vector<hg_size_t> m_packed_val_sizes;

        keyvals()                          = default;
        keyvals(keyvals&&)                 = default;
        keyvals(const keyvals&)            = delete;
        keyvals& operator=(keyvals&&)      = default;
        keyvals& operator=(const keyvals&) = delete;
    };

    typedef std::unordered_map<const sdskv::database*, std::queue<keyvals>> entries_type;

    std::unique_ptr<WriteBatchStatistics> m_stats;
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
        if(!m_stats) return;
        std::lock_guard<tl::mutex> g(m_stats_mtx);
        m_stats->key_sizes.updateWith(ksize);
        if(vsize)
            m_stats->value_sizes.updateWith(vsize);
    }

    void update_operation_statistics(size_t batch_size) {
        if(!m_stats) return;
        std::lock_guard<tl::mutex> g(m_stats_mtx);
        m_stats->batch_sizes.updateWith(batch_size);
    }

    static void writer_thread(WriteBatchImpl& wb,
                              const sdskv::database* db,
                              std::queue<keyvals>& kvs_queue,
                              Exception* exception,
                              char* ok) {
        *ok = 1;
        while(!kvs_queue.empty()) {
            auto& batch = kvs_queue.front();
            try {
                db->put_packed(batch.m_packed_keys, batch.m_packed_key_sizes,
                               batch.m_packed_vals, batch.m_packed_val_sizes);
            } catch(sdskv::exception& ex) {
                if(ex.error() != SDSKV_ERR_KEYEXISTS) {
                    *ok = 0;
                    *exception = Exception(std::string("SDSKV error: ")+ex.what());
                    spdlog::error("SDSKV exception occured in WriteBatch's writer_thread: {}", ex.what());
                }
            }
            wb.update_operation_statistics(batch.m_size);
            kvs_queue.pop();
        }
    }

    static void spawn_writer_threads(WriteBatchImpl& wb, entries_type& entries, tl::pool& pool) {
        auto num_threads = entries.size();
        std::vector<tl::managed<tl::thread>> threads;
        std::vector<Exception> exceptions(num_threads);
        std::vector<char>      oks(num_threads);
        unsigned i=0;
        for(auto& e : entries) {
            char* ok = &oks[i];
            Exception* ex = &exceptions[i];
            threads.push_back(pool.make_thread([&wb, &e, ok, ex]() {
                    writer_thread(wb, e.first, e.second, ex, ok);
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
        bool should_stop;
        {
            std::unique_lock<tl::mutex> lock(batch.m_mutex);
            should_stop = batch.m_async_thread_should_stop;
        }
        while(!(should_stop && batch.m_entries.empty())) {
            std::unique_lock<tl::mutex> lock(batch.m_mutex);
            while(batch.m_entries.empty()) {
                batch.m_cond.wait(lock);
                if(batch.m_entries.empty() && should_stop)
                    break;
            }
            should_stop = batch.m_async_thread_should_stop;
            if(batch.m_entries.empty())
                continue;
            auto entries = std::move(batch.m_entries);
            batch.m_entries.clear();
            lock.unlock();
            spawn_writer_threads(batch, entries, batch.m_async_engine->m_pool);
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
            // find the queue of batches
            std::queue<keyvals>& entry_queue = m_entries[&db];
            if(entry_queue.empty()
            || entry_queue.back().m_size >= m_max_batch_size) {
                entry_queue.emplace();
            }
            auto& kv_batch = entry_queue.back();
            kv_batch.m_packed_keys += product_id.m_key;
            kv_batch.m_packed_key_sizes.push_back(product_id.m_key.size());
            kv_batch.m_size += 1;
            if(vsize != 0) {
                size_t offset = kv_batch.m_packed_vals.size();
                kv_batch.m_packed_vals.resize(offset + vsize);
                std::memcpy(const_cast<char*>(kv_batch.m_packed_vals.data()) + offset, value, vsize);
            }
            kv_batch.m_packed_val_sizes.push_back(vsize);

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
            auto& kvs_queue = m_entries[&db];

            if(kvs_queue.empty()
            || kvs_queue.back().m_size >= m_max_batch_size) {
                kvs_queue.emplace();
            }
            auto& kv_batch = kvs_queue.back();
            size_t offset = kv_batch.m_packed_keys.size();
            kv_batch.m_packed_keys.resize(offset + sizeof(id));
            std::memcpy(const_cast<char*>(kv_batch.m_packed_keys.data())+offset,
                        reinterpret_cast<char*>(&id), sizeof(id));
            kv_batch.m_packed_key_sizes.push_back(sizeof(id));
            kv_batch.m_packed_val_sizes.push_back(0);
            kv_batch.m_size += 1;
            update_keyval_statistics(sizeof(id), 0);
        }
        if(was_empty) {
            m_cond.notify_one();
        }
        return true;
    }

    void flush(bool restart_thread=true) {
        if(!m_async_engine) { // flush everything here
            tl::xstream es = tl::xstream::self();
            spawn_writer_threads(*this, m_entries, es.get_main_pools(1)[0]);
        } else { // wait for AsyncEngine to have flushed everything
            if(m_async_thread.empty())
                return;
            {
                std::lock_guard<tl::mutex> lock(m_mutex);
                m_async_thread_should_stop = true;
            }
            m_cond.notify_all();
            m_async_thread[0]->join();
            m_async_thread.clear();
            if(restart_thread) {
                // thread must be restarted
                m_async_thread.push_back(
                    m_async_engine->m_pool.make_thread([batch=this](){
                        async_writer_thread(*batch);
                    })
                );
            }
        }
    }

    ~WriteBatchImpl() {
        flush(false);
    }

    void collectStatistics(WriteBatchStatistics& stats) const {
        std::unique_lock<tl::mutex> lock(m_stats_mtx);
        if(m_stats)
            stats = *m_stats;
    }
};

}

#endif
