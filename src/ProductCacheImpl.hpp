/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "hepnos/ProductCache.hpp"
#include "ItemDescriptor.hpp"
#include "DataStoreImpl.hpp"
#include <spdlog/spdlog.h>
#include <unordered_map>
#include <unordered_set>
#include <thallium.hpp>

namespace hepnos {

namespace tl = thallium;

struct ProductCacheImpl {

    mutable tl::rwlock                           m_lock;
    std::unordered_map<std::string, std::string> m_map;
    std::unordered_set<std::string>              m_not_found;
    bool                                         m_erase_on_load = false;

    bool loadRawProduct(const ProductID& product_id, std::string& data) {
        if(!m_erase_on_load) m_lock.rdlock();
        else                 m_lock.wrlock();
        auto it = m_map.find(product_id.m_key);
        auto found = it != m_map.end();
        if(found) {
            if(!m_erase_on_load) {
                data = it->second;
            } else {
                data = std::move(it->second);
                m_map.erase(it);
            }
        } else {
            auto it2 = m_not_found.find(product_id.m_key);
            auto not_found = it2 != m_not_found.end();
            if(!not_found) {
                spdlog::warn("Attempted to find product {} in cache, but it was not found. "
                             "Did you set preload for this product type and label?",
                             product_id.toJSON());
            } else if(m_erase_on_load) {
                m_not_found.erase(it2);
            }
        }
        m_lock.unlock();
        //spdlog::trace("[cache] Loading product {} from cache => {}",
        //              product_id.toJSON(), found ? "found" : "not found");
        return found;
    }

    bool loadRawProduct(const ProductID& product_id, char* value, size_t* vsize) {
        if(!m_erase_on_load) m_lock.rdlock();
        else                 m_lock.wrlock();
        auto it = m_map.find(product_id.m_key);
        auto found = it != m_map.end();
        if(found) {
            auto& data = it->second;
            *vsize = data.size();
            if(*vsize) std::memcpy(value, data.data(), *vsize);
            if(m_erase_on_load)
                m_map.erase(it);
        } else {
            auto it2 = m_not_found.find(product_id.m_key);
            auto not_found = it2 != m_not_found.end();
            if(!not_found) {
                spdlog::warn("Attempted to find product {} in cache, but it was not found. "
                             "Did you set preload for this product type and label?",
                             product_id.toJSON());
            } else if(m_erase_on_load) {
                m_not_found.erase(it2);
            }
        }
        m_lock.unlock();
        //spdlog::trace("[cache] Loading product {} from cache => {}",
        //              product_id.toJSON(), found ? "found" : "not found");
        return found;
    }

    bool loadRawProduct(const ItemDescriptor& id,
                        const std::string& productName,
                        std::string& data) {
        auto product_id = DataStoreImpl::buildProductID(id, productName);
        return loadRawProduct(product_id, data);
    }

    bool loadRawProduct(const ItemDescriptor& id,
                        const std::string& productName,
                        char* value, size_t* vsize) {
        auto product_id = DataStoreImpl::buildProductID(id, productName);
        return loadRawProduct(product_id, value, vsize);
    }

    bool hasProduct(const ProductID& product_id) const {
        m_lock.rdlock();
        auto it = m_map.find(product_id.m_key);
        auto found = it != m_map.end();
        m_lock.unlock();
        //spdlog::trace("[cache] Checking product {} from cache => {}",
        //              product_id.toJSON(), found ? "found" : "not found");
        return found;
    }

    bool hasProduct(const ItemDescriptor& id,
                    const std::string& productName) const {
        auto product_id = DataStoreImpl::buildProductID(id, productName);
        return hasProduct(product_id);
    }

    bool checkNotFound(const ProductID& product_id) const {
        m_lock.rdlock();
        auto it = m_not_found.find(product_id.m_key);
        auto ok = it != m_not_found.end();
        m_lock.unlock();
        return ok;
    }

    bool checkNotFound(const ItemDescriptor& id,
                       const std::string& productName) const {
        auto product_id = DataStoreImpl::buildProductID(id, productName);
        return checkNotFound(product_id);
    }

    void addNotFound(const ProductID& product_id) {
        m_lock.wrlock();
        m_not_found.insert(product_id.m_key);
        m_lock.unlock();
    }

    void addNotFound(const ItemDescriptor& id,
                     const std::string& productName) {
        auto product_id = DataStoreImpl::buildProductID(id, productName);
        addNotFound(product_id);
    }

    void addRawProduct(const ProductID& product_id,
                       const std::string& data) {
        m_lock.wrlock();
        m_map[product_id.m_key] = data;
        m_lock.unlock();
        //spdlog::trace("[cache] Added product {} to cache",
        //              product_id.toJSON());
    }

    void addRawProduct(const ItemDescriptor& id,
                       const std::string& productName,
                       const std::string& data) {
        auto product_id = DataStoreImpl::buildProductID(id, productName);
        addRawProduct(product_id, data);
    }

    void addRawProduct(const ProductID& product_id,
                       std::string&& data) {
        m_lock.wrlock();
        m_map[product_id.m_key] = std::move(data);
        m_lock.unlock();
        //spdlog::trace("[cache] Added product {} to cache",
        //              product_id.toJSON());
    }

    void addRawProduct(const ItemDescriptor& id,
                       const std::string& productName,
                       std::string&& data) {
        auto product_id = DataStoreImpl::buildProductID(id, productName);
        addRawProduct(product_id, std::move(data));
    }

    void removeRawProduct(const ProductID& product_id) {
        m_lock.wrlock();
        m_map.erase(product_id.m_key);
        m_not_found.erase(product_id.m_key);
        m_lock.unlock();
        //spdlog::trace("[cache] Removed product {} from cache",
        //              product_id.toJSON());
    }

    void removeRawProduct(const ItemDescriptor& id,
                          const std::string& productName) {
        auto product_id = DataStoreImpl::buildProductID(id, productName);
        removeRawProduct(product_id);
    }
};

}
