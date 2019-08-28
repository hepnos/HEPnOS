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
#include <sdskv-client.h>
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

    struct database {
        sdskv_provider_handle_t m_sdskv_ph;
        sdskv_database_id_t     m_sdskv_db;
    };

    margo_instance_id                         m_mid;          // Margo instance
    std::unordered_map<std::string,hg_addr_t> m_addrs;        // Addresses used by the service
    sdskv_client_t                            m_sdskv_client; // SDSKV client
    std::vector<database>                     m_databases;    // list of SDSKV databases
    struct ch_placement_instance*             m_chi_sdskv;    // ch-placement instance for SDSKV
    const DataStore::iterator                 m_end;          // iterator for the end() of the DataStore

    Impl(DataStore* parent)
    : m_mid(MARGO_INSTANCE_NULL)
    , m_sdskv_client(SDSKV_CLIENT_NULL)
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
        ret = sdskv_client_init(m_mid, &m_sdskv_client);
        if(ret != SDSKV_SUCCESS) {
            cleanup();
            throw Exception("Could not create SDSKV client");
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
                    throw Exception("margo_addr_lookup failed");
                }
                m_addrs[str_addr] = addr;
            }
            // get the number of providers
            uint16_t num_providers = it->second.as<uint16_t>();
            sdskv_provider_handle_t ph;
            for(uint16_t provider_id = 0 ; provider_id < num_providers; provider_id++) {
                ret = sdskv_provider_handle_create(m_sdskv_client, addr, provider_id, &ph);
                if(ret != SDSKV_SUCCESS) {
                    cleanup();
                    throw Exception("sdskv_provider_handle_create failed");
                }
                size_t db_count = 256;
                ret = sdskv_count_databases(ph, &db_count);
                if(ret != SDSKV_SUCCESS) {
                    sdskv_provider_handle_release(ph);
                    cleanup();
                    throw Exception("sdskv_count_databases failed");
                }
                std::cerr << "Found " << db_count << " databases" << std::endl;
                if(db_count == 0) {
                    continue;
                }
                std::vector<sdskv_database_id_t> db_ids(db_count);
                std::vector<char*> db_names(db_count);
                ret = sdskv_list_databases(ph, &db_count, db_names.data(), db_ids.data());
                if(ret != SDSKV_SUCCESS) {
                    sdskv_provider_handle_release(ph);
                    cleanup();
                    throw Exception("sdskv_list_databases failed");
                }
                std::cout << "db_count is now " << db_count << std::endl;
                unsigned i = 0;
                for(auto id : db_ids) {
                    std::cout << "Database: " << id << " " << db_names[i] << std::endl;
                    database db;
                    sdskv_provider_handle_ref_incr(ph);
                    db.m_sdskv_ph = ph;
                    db.m_sdskv_db = id;
                    m_databases.push_back(db);
                    i += 1;
                }
                sdskv_provider_handle_release(ph);
            }
        }
        // initialize ch-placement for the SDSKV providers
        m_chi_sdskv = ch_placement_initialize("hash_lookup3", m_databases.size(), 4, 0);
    }

    void cleanup() {
        for(const auto& db : m_databases) {
            sdskv_provider_handle_release(db.m_sdskv_ph);
        }
        sdskv_client_finalize(m_sdskv_client);
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

    bool load(uint8_t level, const std::string& containerName,
            const std::string& objectName, std::vector<char>& data) const {
        int ret;
        // build full name
        std::stringstream ss;
        if(!containerName.empty())
            ss << containerName << "/";
        ss << objectName;
        // hash the name to get the provider id
        long unsigned sdskv_provider_idx = 0;
        uint64_t name_hash;
        if(level != 0) {
            name_hash = std::hash<std::string>()(containerName);
        } else {
            // use the complete name for final objects (level 0)
            name_hash = std::hash<std::string>()(ss.str());
        }
        ch_placement_find_closest(m_chi_sdskv, name_hash, 1, &sdskv_provider_idx);
        // make corresponding datastore entry
        DataStoreEntryPtr entry = make_datastore_entry(level, ss.str());
        auto& db = m_databases[sdskv_provider_idx];
        auto sdskv_ph = db.m_sdskv_ph;
        auto db_id = db.m_sdskv_db;
        // read the value
        // find the size of the value, as a way to check if the key exists
        hg_size_t vsize;
        ret = sdskv_length(sdskv_ph, db_id, entry->raw(), entry->length(), &vsize);
        if(ret == SDSKV_ERR_UNKNOWN_KEY) {
            return false;
        }
        if(ret != SDSKV_SUCCESS) {
            throw Exception("Error occured when calling sdskv_length");
        }

        data.resize(vsize);
        ret = sdskv_get(sdskv_ph, db_id, entry->raw(), entry->length(), data.data(), &vsize);
        if(ret != SDSKV_SUCCESS) {
            throw Exception("Error occured when calling sdskv_get");
        }
        return true;
    }

    ProductID store(uint8_t level, const std::string& containerName,
            const std::string& objectName, const std::vector<char>& data) {
        // build full name
        std::stringstream ss;
        if(!containerName.empty())
            ss << containerName << "/";
        ss << objectName;
        // Create the product id
        ProductID product_id(level, containerName, objectName);
        // hash the name to get the provider id
        long unsigned sdskv_provider_idx = 0;
        uint64_t name_hash;
        if(level != 0) {
            name_hash = std::hash<std::string>()(containerName);
        } else {
            // use the complete name for final objects (level 0)
            name_hash = std::hash<std::string>()(ss.str());
        }
        ch_placement_find_closest(m_chi_sdskv, name_hash, 1, &sdskv_provider_idx);
        // make corresponding datastore entry key
        DataStoreEntryPtr entry = make_datastore_entry(level, ss.str());
        const auto& sdskv_info = m_databases[sdskv_provider_idx];
        auto sdskv_ph = sdskv_info.m_sdskv_ph;
        auto db_id = sdskv_info.m_sdskv_db;
        // check if the key exists
        hg_size_t vsize;
        int ret = sdskv_length(sdskv_ph, db_id, entry->raw(), entry->length(), &vsize);
        if(ret == HG_SUCCESS) return ProductID(); // key already exists
        if(ret != SDSKV_ERR_UNKNOWN_KEY) { // there was a problem with sdskv
            throw Exception("Could not check if key exists in SDSKV (sdskv_length error)");
        }
        // if it's not a last-level data entry (data product), store in sdskeyval
        ret = sdskv_put(sdskv_ph, db_id, entry->raw(), entry->length(), data.data(), data.size());
        if(ret != SDSKV_SUCCESS) {
            throw Exception("Could not put key/value pair in SDSKV (sdskv_put error)");
        }
        return product_id;
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
        const auto& sdskv_info = m_databases[provider_idx];
        auto ph = sdskv_info.m_sdskv_ph;
        auto db_id = sdskv_info.m_sdskv_db;
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
