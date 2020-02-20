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

class WriteBatchImpl {

    struct keyvals {
        std::vector<std::string> m_keys;
        std::vector<std::string> m_values;
    };

    struct writer_thread_args {
        const sdskv::database* db  = 0;
        keyvals* keyval_list = nullptr;
        Exception ex;
        bool ok              = true;
    };

    static void writer_thread(void* x) {
        writer_thread_args* args = static_cast<writer_thread_args*>(x);
        auto db  = args->db;
        const auto& keys = args->keyval_list->m_keys;
        const auto& vals = args->keyval_list->m_values;
        try {
            db->put_multi(keys, vals);
        } catch(Exception& ex) {
            args->ok = false;
            args->ex = ex;
        }
    }

    public:

    std::shared_ptr<DataStoreImpl>                      m_datastore;
    std::unordered_map<const sdskv::database*, keyvals> m_entries;

    WriteBatchImpl(const std::shared_ptr<DataStoreImpl>& ds)
    : m_datastore(ds) {}

    ProductID storeRawProduct(const ItemDescriptor& id,
                              const std::string& productName,
                              const char* value, size_t vsize)
    {
        // build the key
        auto product_id = m_datastore->buildProductID(id, productName);
        // locate db
        auto& db = m_datastore->locateProductDb(product_id);
        // insert in the map of entries
        keyvals& entry = m_entries[&db];
        entry.m_keys.push_back(product_id.m_key);
        entry.m_values.emplace_back(value, vsize);
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
        // locate db
        auto& db = m_datastore->locateItemDb(id);
        // insert in the map of entries
        auto& entry = m_entries[&db];
        entry.m_keys.push_back(std::string(reinterpret_cast<char*>(&id), sizeof(id)));
        entry.m_values.emplace_back();
        return true;
    }

    void flush() {
        ABT_xstream es = ABT_XSTREAM_NULL;
        ABT_xstream_self(&es);
        auto num_threads = m_entries.size();
        std::vector<ABT_thread> threads(num_threads);
        std::vector<writer_thread_args> args(num_threads);
        unsigned i=0;
        for(auto& e : m_entries) {
            args[i].db = e.first;
            args[i].keyval_list = &e.second;
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

    ~WriteBatchImpl() {
        flush();
    }
};

}

#endif
