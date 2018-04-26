/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRIVATE_DATASTORE_IMPL
#define __HEPNOS_PRIVATE_DATASTORE_IMPL

#include <vector>
#include <unordered_set>
#include <functional>
#include <iostream>
#include <yaml-cpp/yaml.h>
#include <sdskv-client.h>
#include <bake-client.h>
#include <ch-placement.h>
#include "KeyTypes.hpp"
#include "hepnos/Exception.hpp"
#include "hepnos/DataStore.hpp"
#include "hepnos/DataSet.hpp"

namespace hepnos {

////////////////////////////////////////////////////////////////////////////////////////////
// DataStore::Impl implementation
////////////////////////////////////////////////////////////////////////////////////////////

class DataStore::Impl {
    public:

    margo_instance_id                    m_mid;          // Margo instance
    std::unordered_set<hg_addr_t>        m_addrs;        // Addresses used by the service
    sdskv_client_t                       m_sdskv_client; // SDSKV client
    bake_client_t                        m_bake_client;  // BAKE client
    std::vector<sdskv_provider_handle_t> m_sdskv_ph;     // list of SDSKV provider handlers
    std::vector<sdskv_database_id_t>     m_sdskv_db;     // list of SDSKV database ids
    struct ch_placement_instance*        m_chi_sdskv;    // ch-placement instance for SDSKV
    std::vector<bake_provider_handle_t>  m_bake_ph;      // list of BAKE provider handlers
    struct ch_placement_instance*        m_chi_bake;     // ch-placement instance for BAKE
    const DataStore::iterator            m_end;          // iterator for the end() of the DataStore

    Impl(DataStore* parent)
    : m_mid(MARGO_INSTANCE_NULL)
    , m_sdskv_client(SDSKV_CLIENT_NULL)
    , m_chi_sdskv(nullptr)
    , m_bake_client(BAKE_CLIENT_NULL)
    , m_chi_bake(nullptr)
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
        ret = sdskv_client_init(m_mid, &m_sdskv_client);
        if(ret != SDSKV_SUCCESS) {
            cleanup();
            throw Exception("Could not create SDSKV client");
        }
        // initialize BAKE client
        ret = bake_client_init(m_mid, &m_bake_client);
        if(ret != 0) {
            cleanup();
            throw Exception("Could not create BAKE client");
        }
        // create list of sdskv provider handles
        YAML::Node sdskv = config["hepnos"]["providers"]["sdskv"];
        for(YAML::const_iterator it = sdskv.begin(); it != sdskv.end(); it++) {
            std::string str_addr = it->first.as<std::string>();
            hg_addr_t addr;
            hret = margo_addr_lookup(m_mid, str_addr.c_str(), &addr);
            if(hret != HG_SUCCESS) {
                margo_addr_free(m_mid,addr);
                cleanup();
                throw Exception("margo_addr_lookup failed");
            }
            m_addrs.insert(addr);
            if(it->second.IsScalar()) {
                uint16_t provider_id = it->second.as<uint16_t>();
                sdskv_provider_handle_t ph;
                ret = sdskv_provider_handle_create(m_sdskv_client, addr, provider_id, &ph);
                margo_addr_free(m_mid, addr);
                if(ret != SDSKV_SUCCESS) {
                    cleanup();
                    throw Exception("sdskv_provider_handle_create failed");
                }
                m_sdskv_ph.push_back(ph);
            } else if(it->second.IsSequence()) {
                for(YAML::const_iterator pid = it->second.begin(); pid != it->second.end(); pid++) {
                    uint16_t provider_id = pid->second.as<uint16_t>();
                    sdskv_provider_handle_t ph;
                    ret = sdskv_provider_handle_create(m_sdskv_client, addr, provider_id, &ph);
                    margo_addr_free(m_mid, addr);
                    if(ret != SDSKV_SUCCESS) {
                        cleanup();
                        throw Exception("sdskv_provider_handle_create failed");
                    }
                    m_sdskv_ph.push_back(ph);
                }
            }
        }
        // loop over sdskv providers and get the database id
        for(auto ph : m_sdskv_ph) {
            sdskv_database_id_t db_id;
            ret = sdskv_open(ph, "hepnosdb", &db_id);
            if(ret != SDSKV_SUCCESS) {
                cleanup();
                throw Exception("sdskv_open failed to open database");
            }
            m_sdskv_db.push_back(db_id);
        }
        // initialize ch-placement for the SDSKV providers
        m_chi_sdskv = ch_placement_initialize("hash_lookup3", m_sdskv_ph.size(), 4, 0);
        // get list of bake provider handles
        YAML::Node bake = config["hepnos"]["providers"]["bake"];
        for(YAML::const_iterator it = bake.begin(); it != bake.end(); it++) {
            std::string str_addr = it->first.as<std::string>();
            hg_addr_t addr;
            hret = margo_addr_lookup(m_mid, str_addr.c_str(), &addr);
            if(hret != HG_SUCCESS) {
                margo_addr_free(m_mid, addr);
                cleanup();
                throw Exception("margo_addr_lookup failed");
            }
            m_addrs.insert(addr);
            if(it->second.IsScalar()) {
                uint16_t provider_id = it->second.as<uint16_t>();
                bake_provider_handle_t ph;
                ret = bake_provider_handle_create(m_bake_client, addr, provider_id, &ph);
                margo_addr_free(m_mid, addr);
                if(ret != 0) {
                    cleanup();
                    throw Exception("bake_provider_handle_create failed");
                }
                m_bake_ph.push_back(ph);
            } else if(it->second.IsSequence()) {
                for(YAML::const_iterator pid = it->second.begin(); pid != it->second.end(); pid++) {
                    uint16_t provider_id = pid->second.as<uint16_t>();
                    bake_provider_handle_t ph;
                    ret = bake_provider_handle_create(m_bake_client, addr, provider_id, &ph);
                    margo_addr_free(m_mid, addr);
                    if(ret != 0) {
                        cleanup();
                        throw Exception("bake_provider_handle_create failed");
                    }
                    m_bake_ph.push_back(ph);
                }
            }
        }
        // initialize ch-placement for the bake providers
        if(m_bake_ph.size()) {
            m_chi_bake = ch_placement_initialize("hash_lookup3", m_bake_ph.size(), 4, 0);
        }
    }

    void cleanup() {
        for(auto ph : m_sdskv_ph) {
            sdskv_provider_handle_release(ph);
        }
        for(auto ph : m_bake_ph) {
            bake_provider_handle_release(ph);
        }
        sdskv_client_finalize(m_sdskv_client);
        bake_client_finalize(m_bake_client);
        if(m_chi_sdskv)
            ch_placement_finalize(m_chi_sdskv);
        if(m_chi_bake)
            ch_placement_finalize(m_chi_bake);
        if(m_mid) margo_finalize(m_mid);
    }

    private:

    static void checkConfig(YAML::Node& config) {
        auto hepnosNode = config["hepnos"];
        if(!hepnosNode) {
            throw Exception("\"hepnos\" entry not found in YAML file");
        }
        auto clientNode = hepnosNode["client"];
        if(!clientNode) {
            throw Exception("\"client\" entry not found in \"hepnos\" section");
        }
        auto protoNode = clientNode["protocol"];
        if(!protoNode) {
            throw Exception("\"protocol\" entry not found in \"client\" section");
        }
        auto providersNode = hepnosNode["providers"];
        if(!providersNode) {
            throw Exception("\"providers\" entry not found in \"hepnos\" section");
        }
        auto sdskvNode = providersNode["sdskv"];
        if(!sdskvNode) {
            throw Exception("\"sdskv\" entry not found in \"providers\" section");
        }
        if(sdskvNode.size() == 0) {
            throw Exception("No provider found in \"sdskv\" section");
        }
        for(auto it = sdskvNode.begin(); it != sdskvNode.end(); it++) {
            if(it->second.IsScalar()) continue; // one provider id given
            if(it->second.IsSequence()) { // array of provider ids given
                if(it->second.size() == 0) {
                    throw Exception("Empty array of provider ids encountered in \"sdskv\" section");
                }
                for(auto pid = it->second.begin(); pid != it->second.end(); pid++) {
                    if(!pid->second.IsScalar()) {
                        throw Exception("Non-scalar provider id encountered in \"sdskv\" section");
                    }
                }
            } else {
                throw Exception("Invalid value type for provider in \"sdskv\" section");
            }
        }
        // bake providers are not mandatory. If they are not present,
        // objects will be stored in sdskv providers.
        auto bakeNode = providersNode["bake"];
        if(!bakeNode) return;
        if(bakeNode.size() == 0) return;
        for(auto it = bakeNode.begin(); it != bakeNode.end(); it++) {
            if(it->second.IsScalar()) continue; // one provider id given
            if(it->second.IsSequence()) { // array of provider ids given
                if(it->second.size() == 0) {
                    throw Exception("No provider found in \"bake\" section");
                }
                for(auto pid = it->second.begin(); pid != it->second.end(); pid++) {
                    if(!pid->second.IsScalar()) {
                        throw Exception("Non-scalar provider id encountered in \"bake\" section");
                    }
                }
            } else {
                throw Exception("Invalid value type for provider in \"bake\" section");
            }
        }
    }

    public:

    bool load(uint8_t level, const std::string& containerName,
            const std::string& objectName, std::vector<char>& data) const {
        int ret;
        // build full name
        std::stringstream ss;
        if(!containerName.empty())
            ss << containerName << "/";
        ss << objectName;
        // hash the name to get the provider id
        long unsigned provider_idx = 0;
        if(level != 0) {
            uint64_t h = std::hash<std::string>()(containerName);
            ch_placement_find_closest(m_chi_sdskv, h, 1, &provider_idx);
        } else {
            // use the complete name for final objects (level 255)
            uint64_t h = std::hash<std::string>()(ss.str());
            ch_placement_find_closest(m_chi_sdskv, h, 1, &provider_idx);
        }
        // make corresponding datastore entry
        DataStoreEntryPtr entry = make_datastore_entry(level, ss.str());
        auto ph = m_sdskv_ph[provider_idx];
        auto db_id = m_sdskv_db[provider_idx];
        // find the size of the value, as a way to check if the key exists
        hg_size_t vsize;
        ret = sdskv_length(ph, db_id, entry->raw(), entry->length(), &vsize);
        if(ret == SDSKV_ERR_UNKNOWN_KEY) {
            return false;
        }
        if(ret != SDSKV_SUCCESS) {
            throw Exception("Error occured when calling sdskv_length");
        }
        // read the value
        data.resize(vsize);
        ret = sdskv_get(ph, db_id, entry->raw(), entry->length(), data.data(), &vsize);
        if(ret != SDSKV_SUCCESS) {
            throw Exception("Error occured when calling sdskv_get");
        }
        return true;
    }

    bool store(uint8_t level, const std::string& containerName,
            const std::string& objectName, const std::vector<char>& data) {
        // build full name
        std::stringstream ss;
        if(!containerName.empty())
            ss << containerName << "/";
        ss << objectName;
        // hash the name to get the provider id
        long unsigned provider_idx = 0;
        if(level != 0) {
            uint64_t h = std::hash<std::string>()(containerName);
            ch_placement_find_closest(m_chi_sdskv, h, 1, &provider_idx);
        } else {
            // use the complete name for final objects (level 0)
            uint64_t h = std::hash<std::string>()(ss.str());
            ch_placement_find_closest(m_chi_sdskv, h, 1, &provider_idx);
        }
        // make corresponding datastore entry
        DataStoreEntryPtr entry = make_datastore_entry(level, ss.str());
        auto ph = m_sdskv_ph[provider_idx];
        auto db_id = m_sdskv_db[provider_idx];
        // check if the key exists
        hg_size_t vsize;
        int ret = sdskv_length(ph, db_id, entry->raw(), entry->length(), &vsize);
        if(ret == HG_SUCCESS) return false; // key already exists
        if(ret != SDSKV_ERR_UNKNOWN_KEY) { // there was a problem with sdskv
            throw Exception("Could not check if key exists in SDSKV (sdskv_length error)");
        }
        ret = sdskv_put(ph, db_id, entry->raw(), entry->length(), data.data(), data.size());
        if(ret != SDSKV_SUCCESS) {
            throw Exception("Could not put key/value pair in SDSKV (sdskv_put error)");
        }
        return true;
    }

    size_t nextKeys(uint8_t level, const std::string& containerName,
            const std::string& lower,
            std::vector<std::string>& keys, size_t maxKeys) const {
        int ret;
        if(level == 0) return 0; // cannot iterate at object level
        // build full name from lower bound key
        std::stringstream ss;
        if(!containerName.empty())
            ss << containerName << "/";
        ss << lower;
        // hash the name to get the provider id
        long unsigned provider_idx = 0;
        uint64_t h = std::hash<std::string>()(containerName);
        ch_placement_find_closest(m_chi_sdskv, h, 1, &provider_idx);
        // make an entry for the lower bound
        DataStoreEntryPtr lb_entry = make_datastore_entry(level, ss.str());
        // create data structures to receive keys
        std::vector<DataStoreEntryPtr> keys_ent;
        std::vector<void*>             keys_ptr(maxKeys);
        std::vector<hg_size_t>         keys_len(maxKeys);
        for(unsigned i=0; i < maxKeys; i++) {
            keys_ent.push_back(make_datastore_entry(level, 1024));
            keys_ptr[i] = keys_ent[i]->raw();
            keys_len[i] = sizeof(DataStoreEntry) + 1024;
        }
        // get provider and database
        auto ph = m_sdskv_ph[provider_idx];
        auto db_id = m_sdskv_db[provider_idx];
        // issue an sdskv_list_keys
        hg_size_t max_keys = maxKeys;
        ret = sdskv_list_keys(ph, db_id, lb_entry->raw(), lb_entry->length(),
                keys_ptr.data(), keys_len.data(), &max_keys);
        if(ret != HG_SUCCESS) {
            throw Exception("Error occured when calling sdskv_list_keys");
        }
        unsigned i = max_keys - 1;
        if(max_keys == 0) return 0;
        // remove keys that don't have the same level or the same prefix
        std::string prefix = containerName + "/";
        keys.resize(0);
        for(unsigned i = 0; i < max_keys; i++) {
            if(keys_ent[i]->m_level != level) {
                max_keys = i;
                break;
            }
            if(!containerName.empty()) {
                size_t lenpre = prefix.size();
                size_t lenstr = strlen(keys_ent[i]->m_fullname);
                if(lenstr < lenpre) {
                    max_keys = i;
                    break;
                }
                if(strncmp(prefix.c_str(), keys_ent[i]->m_fullname, lenpre) != 0) {
                    max_keys = i;
                    break;
                }
            }
            keys.push_back(keys_ent[i]->m_fullname);
        }
        // set the resulting keys
        return max_keys;
    }
};

}

#endif
