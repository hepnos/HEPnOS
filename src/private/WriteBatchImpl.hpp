/*
 * (C) 2019 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRIVATE_WRITEBATCH_IMPL_H
#define __HEPNOS_PRIVATE_WRITEBATCH_IMPL_H

#include <iostream>
#include <unordered_map>
#include <string>
#include <vector>
#include "hepnos/WriteBatch.hpp"
#include "DataStoreImpl.hpp"

namespace hepnos {

class WriteBatch::Impl {

    struct writer_thread_args {
        unsigned long db_idx = 0;
        std::pair<std::vector<std::string>, std::vector<std::string>>* keyvals = nullptr;
        Impl* batch = nullptr;
        Exception ex;
        bool ok = true;
    };

    static void writer_thread(void* x) {
        writer_thread_args* args = static_cast<writer_thread_args*>(x);
        auto batch   = args->batch;
        auto db_idx  = args->db_idx;
        auto keyvals = args->keyvals;
        const auto& keys = keyvals->first;
        const auto& vals = keyvals->second;
        try {
            batch->m_datastore->m_impl->storeMultiple(db_idx, keys, vals);
        } catch(Exception& ex) {
            args->ok = false;
            args->ex = ex;
        }
    }

    public:

        DataStore*   m_datastore;
        std::unordered_map<unsigned long, 
            std::pair<std::vector<std::string>,
                      std::vector<std::string>>> m_entries;

        Impl(DataStore* ds)
        : m_datastore(ds) {}

        ProductID store(uint8_t level, const std::string& containerName, const std::string& objectName, const std::string& content) {
            std::string key = DataStore::Impl::buildKey(level, containerName, objectName);
            auto db_idx = m_datastore->m_impl->computeDbIndex(level, containerName, key);
            auto& e = m_entries[db_idx];
            e.first.push_back(std::move(key));
            e.second.push_back(content);
            return ProductID(level, containerName, objectName);
        }

        ProductID store(uint8_t level, const std::string& containerName, const std::string& objectName, std::string&& content) {
            std::string key = DataStore::Impl::buildKey(level, containerName, objectName);
            auto db_idx = m_datastore->m_impl->computeDbIndex(level, containerName, key);
            auto& e = m_entries[db_idx];
            e.first.push_back(std::move(key));
            e.second.push_back(std::move(content));
            return ProductID(level, containerName, objectName);
        }

        void flush() {
            ABT_xstream es = ABT_XSTREAM_NULL;
            ABT_xstream_self(&es);
            auto num_threads = m_entries.size();
            std::vector<ABT_thread> threads(num_threads);
            std::vector<writer_thread_args> args(num_threads);
            unsigned i=0;
            for(auto& e : m_entries) {
                auto db_idx = e.first;
                args[i].db_idx = e.first;
                args[i].batch = this;
                args[i].keyvals = &e.second;
                ABT_thread_create_on_xstream(es, &writer_thread, &args[i], ABT_THREAD_ATTR_NULL, &threads[i]);
                i += 1;
            }
            ABT_thread_join_many(num_threads, threads.data());
            ABT_thread_free_many(num_threads, threads.data());
            for(auto& a : args) {
                if(not a.ok) throw a.ex;
            }
            m_entries.clear();
        }

        ~Impl() {
            flush();
        }
};

}

#endif
