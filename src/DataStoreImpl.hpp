/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRIVATE_DATASTORE_IMPL
#define __HEPNOS_PRIVATE_DATASTORE_IMPL

#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <functional>
#include <iostream>
#include <yaml-cpp/yaml.h>
#include <sdskv-client.hpp>
#include <ch-placement.h>
#include "hepnos/Exception.hpp"
#include "hepnos/DataStore.hpp"
#include "hepnos/DataSet.hpp"
#include "StringHash.hpp"

namespace hepnos {

////////////////////////////////////////////////////////////////////////////////////////////
// DataStore::Impl implementation
////////////////////////////////////////////////////////////////////////////////////////////

class DataStore::Impl {
    public:

    margo_instance_id                         m_mid;          // Margo instance
    std::unordered_map<std::string,hg_addr_t> m_addrs;        // Addresses used by the service
    sdskv::client                             m_sdskv_client; // SDSKV client
    std::vector<sdskv::database>              m_databases;    // list of SDSKV databases
    struct ch_placement_instance*             m_chi_sdskv;    // ch-placement instance for SDSKV
    const DataStore::iterator                 m_end;          // iterator for the end() of the DataStore

    Impl()
    : m_mid(MARGO_INSTANCE_NULL)
    , m_chi_sdskv(nullptr)
    , m_end() {}

    void init(const std::string& configFile) {
        int ret;
        hg_return_t hret;
        YAML::Node config = YAML::LoadFile(configFile);
        checkConfig(config);
        // get protocol
        std::string proto = config["hepnos"]["client"]["protocol"].as<std::string>();
        // initialize Margo
        m_mid = margo_init(proto.c_str(), MARGO_CLIENT_MODE, 0, 0);
        if(!m_mid) {
            cleanup();
            throw Exception("Could not initialized Margo");
        }
        // initialize SDSKV client
        try {
            m_sdskv_client = sdskv::client(m_mid);
        } catch(sdskv::exception& ex) {
            cleanup();
            throw Exception("Could not create SDSKV client (SDSKV error="+std::to_string(ex.error())+")");
        }
        // create list of sdskv provider handles
        YAML::Node sdskv = config["hepnos"]["providers"]["sdskv"];
        for(YAML::const_iterator it = sdskv.begin(); it != sdskv.end(); it++) {
            std::string str_addr = it->first.as<std::string>();
            hg_addr_t addr;
            if(m_addrs.count(str_addr) != 0) {
                addr = m_addrs[str_addr];
            } else {
                hret = margo_addr_lookup(m_mid, str_addr.c_str(), &addr);
                if(hret != HG_SUCCESS) {
                    margo_addr_free(m_mid,addr);
                    cleanup();
                    throw Exception("margo_addr_lookup failed (MARGO error="+std::to_string(hret)+")");
                }
                m_addrs[str_addr] = addr;
            }
            // get the number of providers
            uint16_t num_providers = it->second.as<uint16_t>();
            for(uint16_t provider_id = 0 ; provider_id < num_providers; provider_id++) {
                std::vector<sdskv::database> dbs;
                try {
                    sdskv::provider_handle ph(m_sdskv_client, addr, provider_id);
                    dbs = m_sdskv_client.open(ph);
                } catch(sdskv::exception& ex) {
                    cleanup();
                    throw Exception("Could not open databases (SDSKV error="+std::to_string(ex.error())+")");
                }
                if(dbs.size() == 0) {
                    continue;
                }
                for(auto& db : dbs)
                    m_databases.push_back(db);
            }
        }
        // initialize ch-placement for the SDSKV providers
        m_chi_sdskv = ch_placement_initialize("hash_lookup3", m_databases.size(), 4, 0);
    }

    void cleanup() {
        m_databases.clear();
        m_sdskv_client = sdskv::client();
        if(m_chi_sdskv)
            ch_placement_finalize(m_chi_sdskv);
        for(auto& addr : m_addrs) {
            margo_addr_free(m_mid, addr.second);
        }
        if(m_mid) margo_finalize(m_mid);
    }

    private:

    static void checkConfig(YAML::Node& config) {
        // config file starts with hepnos entry
        auto hepnosNode = config["hepnos"];
        if(!hepnosNode) {
            throw Exception("\"hepnos\" entry not found in YAML file");
        }
        // hepnos entry has client entry
        auto clientNode = hepnosNode["client"];
        if(!clientNode) {
            throw Exception("\"client\" entry not found in \"hepnos\" section");
        }
        // client entry has protocol entry
        auto protoNode = clientNode["protocol"];
        if(!protoNode) {
            throw Exception("\"protocol\" entry not found in \"client\" section");
        }
        // hepnos entry has providers entry
        auto providersNode = hepnosNode["providers"];
        if(!providersNode) {
            throw Exception("\"providers\" entry not found in \"hepnos\" section");
        }
        // provider entry has sdskv entry
        auto sdskvNode = providersNode["sdskv"];
        if(!sdskvNode) {
            throw Exception("\"sdskv\" entry not found in \"providers\" section");
        }
        // sdskv entry is not empty
        if(sdskvNode.size() == 0) {
            throw Exception("No provider found in \"sdskv\" section");
        }
        // for each sdskv entry
        for(auto it = sdskvNode.begin(); it != sdskvNode.end(); it++) {
            if(it->second.IsScalar()) continue; // one provider id given
            else {
                throw Exception("Invalid value type for provider in \"sdskv\" section");
            }
        }
    }

    public:

    static inline std::string buildKey(
            uint8_t level,
            const std::string& containerName,
            const std::string& objectName) {
        size_t c = 1 + objectName.size();
        if(!containerName.empty()) c += containerName.size() + 1;
        std::string result(c,'\0');
        result[0] = level;
        if(!containerName.empty()) {
            std::memcpy(&result[1], containerName.data(), containerName.size());
            size_t x = 1+containerName.size();
            result[x] = '/';
            std::memcpy(&result[x+1], objectName.data(), objectName.size());
        } else {
            std::memcpy(&result[1], objectName.data(), objectName.size());
        }
        return result;
    }

    unsigned long computeDbIndex(uint8_t level, const std::string& containerName, const std::string& key) const {
        // hash the name to get the provider id
        long unsigned sdskv_db_idx = 0;
        uint64_t name_hash;
        if(level != 0) {
            name_hash = hashString(containerName);
        } else {
            // use the complete name for final objects (level 0)
            name_hash = hashString(key);
        }
        ch_placement_find_closest(m_chi_sdskv, name_hash, 1, &sdskv_db_idx);
        return sdskv_db_idx;
    }

    bool load(uint8_t level, const std::string& containerName,
            const std::string& objectName, std::string& data) const {
        int ret;
        // build key
        auto key = buildKey(level, containerName, objectName);
        // find out which DB to access
        long unsigned sdskv_db_idx = computeDbIndex(level, containerName, key);
        // make corresponding datastore entry
        auto& db = m_databases[sdskv_db_idx];
        // read the value
        if(data.size() == 0)
            data.resize(2048); // eagerly allocate 2KB
        try {
            db.get(key, data);
        } catch(sdskv::exception& ex) {
            if(ex.error() == SDSKV_ERR_UNKNOWN_KEY)
                return false;
            else
                throw Exception("Error occured when calling sdskv::database::get (SDSKV error="+std::to_string(ex.error())+")");
        }
        return true;
    }

    bool load(uint8_t level, const std::string& containerName,
            const std::string& objectName, char* value, size_t* vsize) const {
        int ret;
        // build key
        auto key = buildKey(level, containerName, objectName);
        // find out which DB to access
        long unsigned sdskv_db_idx = computeDbIndex(level, containerName, key);
        // make corresponding datastore entry
        auto& db = m_databases[sdskv_db_idx];
        // read the value
        try {
            db.get(key.data(), key.size(), value, vsize);
        } catch(sdskv::exception& ex) {
            if(ex.error() == SDSKV_ERR_UNKNOWN_KEY)
                return false;
            else if(ex.error() == SDSKV_ERR_SIZE)
                return false;
            else
                throw Exception("Error occured when calling sdskv::database::get (SDSKV error="+std::to_string(ex.error())+")");
        }
        return true;
    }

    bool exists(uint8_t level, const std::string& containerName,
            const std::string& objectName) const {
        int ret;
        // build key
        auto key = buildKey(level, containerName, objectName);
        // find out which DB to access
        long unsigned sdskv_db_idx = computeDbIndex(level, containerName, key);
        // make corresponding datastore entry
        auto& db = m_databases[sdskv_db_idx];
        try {
            return db.exists(key);
        } catch(sdskv::exception& ex) {
            throw Exception("Error occured when calling sdskv::database::exists (SDSKV error="+std::to_string(ex.error())+")");
        }
        return false;
    }

    ProductID store(uint8_t level, const std::string& containerName,
            const std::string& objectName, const char* data=nullptr, size_t data_size=0) {
        // build full name
        auto key = buildKey(level, containerName, objectName);
        // find out which DB to access
        long unsigned sdskv_db_idx = computeDbIndex(level, containerName, key);
        // Create the product id
        const auto& db = m_databases[sdskv_db_idx];
        try {
            db.put(key.data(), key.size(), data, data_size);
        } catch(sdskv::exception& ex) {
            if(ex.error() == SDSKV_ERR_KEYEXISTS) {
                return ProductID();
            } else {
                throw Exception("Error occured when calling sdskv::database::put (SDSKV error=" +std::to_string(ex.error()) + ")");
            }
        }
        return ProductID(level, containerName, objectName);
    }

    void storeMultiple(unsigned long db_index, 
            const std::vector<std::string>& keys,
            const std::vector<std::string>& values) {
        // Create the product id
        const auto& db = m_databases[db_index];
        try {
            db.put_multi(keys, values);
        } catch(sdskv::exception& ex) {
            throw Exception("Error occured when calling sdskv::database::put (SDSKV error=" +std::to_string(ex.error()) + ")");
        }
    }

    size_t nextKeys(uint8_t level, const std::string& containerName,
            const std::string& lower,
            std::vector<std::string>& keys, size_t maxKeys) const {
        int ret;
        if(level == 0) return 0; // cannot iterate at object level
        // hash the name to get the provider id
        long unsigned db_idx = 0;
        uint64_t h = hashString(containerName);
        ch_placement_find_closest(m_chi_sdskv, h, 1, &db_idx);
        // make an entry for the lower bound
        auto lb_entry = buildKey(level, containerName, lower);
        // get provider and database
        const auto& db = m_databases[db_idx];
        // ignore keys that don't have the same level or the same prefix
        std::string prefix(2+containerName.size(), '\0');
        prefix[0] = level;
        if(containerName.size() != 0) {
            std::memcpy(&prefix[1], containerName.data(), containerName.size());
            prefix[prefix.size()-1] = '/';
        } else {
            prefix.resize(1);
        }
        // issue an sdskv_list_keys
        std::vector<std::string> entries(maxKeys);
        try {
            db.list_keys(lb_entry, prefix, entries);
        } catch(sdskv::exception& ex) {
            throw Exception("Error occured when calling sdskv::database::list_keys (SDSKV error="+std::string(ex.what()) + ")");
        }
        keys.resize(0);
        for(const auto& entry : entries) {
            keys.emplace_back(&entry[1], entry.size()-1);
        }
        return keys.size();
    }
};

}

#endif