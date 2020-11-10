/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "hepnos/ProductCache.hpp"
#include "ItemDescriptor.hpp"
#include "DataStoreImpl.hpp"
#include <unordered_map>
#include <thallium.hpp>

namespace hepnos {

namespace tl = thallium;

struct ProductCacheImpl {

    mutable tl::rwlock                           m_lock;
    std::unordered_map<std::string, std::string> m_map;

    bool loadRawProduct(const ProductID& product_id, std::string& data) const {
        m_lock.rdlock();
        auto it = m_map.find(product_id.m_key);
        auto found = it != m_map.end();
        if(found) {
            data = it->second;
        }
        m_lock.unlock();
        return found;
    }

    bool loadRawProduct(const ProductID& product_id, char* value, size_t* vsize) const {
        m_lock.rdlock();
        auto it = m_map.find(product_id.m_key);
        auto found = it != m_map.end();
        if(found) {
            auto& data = it->second;
            *vsize = data.size();
            if(*vsize) std::memcpy(value, data.data(), *vsize);
        }
        m_lock.unlock();
        return found;
    }

    bool loadRawProduct(const ItemDescriptor& id,
                        const std::string& productName,
                        std::string& data) const {
        auto product_id = DataStoreImpl::buildProductID(id, productName);
        return loadRawProduct(product_id, data);
    }

    bool loadRawProduct(const ItemDescriptor& id,
                        const std::string& productName,
                        char* value, size_t* vsize) const {
        auto product_id = DataStoreImpl::buildProductID(id, productName);
        return loadRawProduct(product_id, value, vsize);
    }

    bool hasProduct(const ProductID& product_id) const {
        m_lock.rdlock();
        auto it = m_map.find(product_id.m_key);
        auto found = it != m_map.end();
        m_lock.unlock();
        return found;
    }

    bool hasProduct(const ItemDescriptor& id,
                    const std::string& productName) const {
        auto product_id = DataStoreImpl::buildProductID(id, productName);
        return hasProduct(product_id);
    }

    void addRawProduct(const ProductID& product_id,
                       const std::string& data) {
        m_lock.wrlock();
        m_map[product_id.m_key] = data;
        m_lock.unlock();
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
        m_lock.unlock();
    }

    void removeRawProduct(const ItemDescriptor& id,
                          const std::string& productName) {
        auto product_id = DataStoreImpl::buildProductID(id, productName);
        removeRawProduct(product_id);
    }
};

}
