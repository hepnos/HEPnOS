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
#include <bake-client.h>
#include <ch-placement.h>
#include "KeyTypes.hpp"
#include "ValueTypes.hpp"
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

    struct storage {
        bake_provider_handle_t m_bake_ph;
        bake_target_id_t       m_bake_target;
    };

    margo_instance_id                         m_mid;          // Margo instance
    std::unordered_map<std::string,hg_addr_t> m_addrs;        // Addresses used by the service
    sdskv_client_t                            m_sdskv_client; // SDSKV client
    bake_client_t                             m_bake_client;  // BAKE client
    std::vector<database>                     m_databases;    // list of SDSKV databases
    struct ch_placement_instance*             m_chi_sdskv;    // ch-placement instance for SDSKV
    std::vector<storage>                      m_storage;      // list of BAKE storage targets
    struct ch_placement_instance*             m_chi_bake;     // ch-placement instance for BAKE
    const DataStore::iterator                 m_end;          // iterator for the end() of the DataStore

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

        // get list of bake provider handles
        YAML::Node bake = config["hepnos"]["providers"]["bake"];
        if(bake) {
            for(YAML::const_iterator it = bake.begin(); it != bake.end(); it++) {
                // get the address of a bake provider
                std::string str_addr = it->first.as<std::string>();
                hg_addr_t addr;
                if(m_addrs.count(str_addr) != 0) {
                    addr = m_addrs[str_addr];
                } else {
                    // lookup the address
                    hret = margo_addr_lookup(m_mid, str_addr.c_str(), &addr);
                    if(hret != HG_SUCCESS) {
                        margo_addr_free(m_mid, addr);
                        cleanup();
                        throw Exception("margo_addr_lookup failed");
                    }
                    m_addrs[str_addr] = addr;
                }
                uint16_t num_providers = it->second.as<uint16_t>();
                for(uint16_t provider_id = 0; provider_id < num_providers; provider_id++) {
                    bake_provider_handle_t ph;
                    ret = bake_provider_handle_create(m_bake_client, addr, provider_id, &ph);
                    if(ret != 0) {
                        cleanup();
                        throw Exception("bake_provider_handle_create failed");
                    }
                    uint64_t num_targets;
                    std::vector<bake_target_id_t> targets(256);
                    ret = bake_probe(ph, 256, targets.data(), &num_targets);
                    if(ret != 0) {
                        bake_provider_handle_release(ph);
                        cleanup();
                        throw Exception("bake_probe failed");
                    }
                    targets.resize(num_targets);
                    for(const auto& id : targets) {
                        storage tgt;
                        bake_provider_handle_ref_incr(ph);
                        tgt.m_bake_ph = ph;
                        tgt.m_bake_target = id;
                        m_storage.push_back(tgt);
                    }
                    bake_provider_handle_release(ph);
                }
            } // for loop
            // find out the bake targets at each bake provider
        }
        // initialize ch-placement for the bake providers
        if(m_storage.size()) {
            m_chi_bake = ch_placement_initialize("hash_lookup3", m_storage.size(), 4, 0);
        }
    }

    void cleanup() {
        for(const auto& db : m_databases) {
            sdskv_provider_handle_release(db.m_sdskv_ph);
        }
        for(const auto& tgt : m_storage) {
            bake_provider_handle_release(tgt.m_bake_ph);
        }
        sdskv_client_finalize(m_sdskv_client);
        bake_client_finalize(m_bake_client);
        if(m_chi_sdskv)
            ch_placement_finalize(m_chi_sdskv);
        if(m_chi_bake)
            ch_placement_finalize(m_chi_bake);
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
        // bake providers are not mandatory. If they are not present,
        // objects will be stored in sdskv providers.
        auto bakeNode = providersNode["bake"];
        if(!bakeNode) return;
        if(bakeNode.size() == 0) return;
        for(auto it = bakeNode.begin(); it != bakeNode.end(); it++) {
            if(it->second.IsScalar()) continue; // one provider id given
            else {
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
        if(level != 0 || m_storage.empty()) { // read directly from sdskv
            
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

        } else { // read from BAKE

            // first get the key/val from sdskv
            DataStoreValue rid_info;
            hg_size_t vsize = sizeof(rid_info);
            ret = sdskv_get(sdskv_ph, db_id, entry->raw(), entry->length(), (void*)(&rid_info), &vsize);
            if(ret == SDSKV_ERR_UNKNOWN_KEY) {
                return false;
            }
            if(ret != SDSKV_SUCCESS) {
                throw Exception("Error occured when calling sdskv_get");
            }
            if(vsize != sizeof(rid_info)) {
                throw Exception("Call to sdskv_get returned a value of unexpected size");
            }
            // now read the data from bake
            data.resize(rid_info.getDataSize());
            if(data.size() == 0) return true;
            long unsigned bake_provider_idx = 0;
            ch_placement_find_closest(m_chi_bake, name_hash, 1, &bake_provider_idx);
            auto& bake_info = m_storage[bake_provider_idx];
            auto bake_ph = bake_info.m_bake_ph;
            auto target = bake_info.m_bake_target;
            uint64_t bytes_read = 0;
            ret = bake_read(bake_ph, rid_info.getBakeRegionID(), 0, data.data(), data.size(), &bytes_read);
            if(ret != BAKE_SUCCESS) {
                throw Exception("Couldn't read region from BAKE");
            }
            if(bytes_read != rid_info.getDataSize()) {
                throw Exception("Bytes read from BAKE did not match expected object size");
            }
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
        if(level != 0 || m_storage.empty()) {
            ret = sdskv_put(sdskv_ph, db_id, entry->raw(), entry->length(), data.data(), data.size());
            if(ret != SDSKV_SUCCESS) {
                throw Exception("Could not put key/value pair in SDSKV (sdskv_put error)");
            }
        } else { // store data in bake
            long unsigned bake_provider_idx = 0;
            ch_placement_find_closest(m_chi_bake, name_hash, 1, &bake_provider_idx);
            const auto& bake_info = m_storage[bake_provider_idx];
            auto bake_ph = bake_info.m_bake_ph;
            auto target = bake_info.m_bake_target;
            bake_region_id_t rid;
            ret = bake_create_write_persist(bake_ph, target, data.data(), data.size(), &rid);
            if(ret != BAKE_SUCCESS) {
                throw Exception("Could not create bake region (bake_create_write_persist error)");
            }
            // create Value to put in SDSKV
            DataStoreValue value(data.size(), bake_provider_idx, rid);
            ret = sdskv_put(sdskv_ph, db_id, entry->raw(), entry->length(), (void*)(&value), sizeof(value));
            if(ret != SDSKV_SUCCESS) {
                ret = bake_remove(bake_ph, rid);
                if(ret != BAKE_SUCCESS) {
                    throw Exception("Dude, not only did SDSKV fail to put the key, but I couldn't cleanup BAKE. Is it Friday 13?");
                }
                throw Exception("Could not put key/value pair in SDSKV (sdskv_put error)");
            }
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
