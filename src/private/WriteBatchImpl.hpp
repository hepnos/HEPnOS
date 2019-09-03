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
            for(const auto& e : m_entries) {
                auto db_idx = e.first;
                const auto& keys = e.second.first;
                const auto& vals = e.second.second;
                m_datastore->m_impl->storeMultiple(db_idx, keys, vals);
            }
        }

        ~Impl() {
            flush();
        }
};

}

#endif
