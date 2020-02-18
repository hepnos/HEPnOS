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
#include "DataSetImpl.hpp"
#include "hepnos/UUID.hpp"

namespace hepnos {

inline void object_resize(hepnos::UUID& uuid, size_t new_size) {
    (void)uuid;
    (void)new_size;
}

inline void* object_data(hepnos::UUID& uuid) {
    return uuid.data;
}

inline const void* object_data(const hepnos::UUID& uuid) {
    return uuid.data;
}

inline size_t object_size(const hepnos::UUID& uuid) {
    return sizeof(uuid);
}

////////////////////////////////////////////////////////////////////////////////////////////
// DataStoreImpl implementation
////////////////////////////////////////////////////////////////////////////////////////////

struct DistributedDBInfo {
    std::vector<sdskv::database>  dbs;
    struct ch_placement_instance* chi = nullptr;
};

class DataStoreImpl {
    public:

    margo_instance_id                         m_mid;          // Margo instance
    std::unordered_map<std::string,hg_addr_t> m_addrs;        // Addresses used by the service
    sdskv::client                             m_sdskv_client; // SDSKV client
    DistributedDBInfo                         m_databases;    // list of SDSKV databases
    DistributedDBInfo                         m_dataset_dbs;  // list of SDSKV databases for DataSets

    DataStoreImpl()
    : m_mid(MARGO_INSTANCE_NULL)
    {}

    void populateDatabases(DistributedDBInfo& db_info, const YAML::Node& db_config) {
        int ret;
        hg_return_t hret;
        for(YAML::const_iterator address_it = db_config.begin(); address_it != db_config.end(); address_it++) {
            std::string str_addr = address_it->first.as<std::string>();
            YAML::Node providers = address_it->second;
            // lookup the address
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
            // iterate over providers for this address
            for(YAML::const_iterator provider_it = providers.begin(); provider_it != providers.end(); provider_it++) {
                // get the provider id
                uint16_t provider_id = provider_it->first.as<uint16_t>();
                // create provider handle
                sdskv::provider_handle ph(m_sdskv_client, addr, provider_id);
                // get the database ids
                YAML::Node databases = provider_it->second;
                // iterate over databases for this provider
                for(unsigned i=0; i < databases.size(); i++) {
                    db_info.dbs.push_back(sdskv::database(ph, databases[i].as<uint64_t>()));
                }
            } // for each provider
        } // for each address
        // initialize ch-placement
        db_info.chi = ch_placement_initialize("hash_lookup3", db_info.dbs.size(), 4, 0);
    }

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
        // populate database info structures for each type of database
        YAML::Node databases = config["hepnos"]["databases"];
        YAML::Node dataset_db = databases["datasets"];
        YAML::Node run_db     = databases["runs"];
        YAML::Node subrun_db  = databases["subruns"];
        YAML::Node event_db   = databases["events"];
        YAML::Node product_db = databases["products"];
        populateDatabases(m_dataset_dbs, dataset_db);
        populateDatabases(m_databases, product_db); // TODO change this
    }

    void cleanup() {
        m_databases.dbs.clear();
        m_sdskv_client = sdskv::client();
        if(m_databases.chi)
            ch_placement_finalize(m_databases.chi);
        if(m_dataset_dbs.chi)
            ch_placement_finalize(m_dataset_dbs.chi);
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
        // hepnos entry has databases entry
        auto databasesNode = hepnosNode["databases"];
        if(!databasesNode) {
            throw Exception("\"databasess\" entry not found in \"hepnos\" section");
        }
        if(!databasesNode.IsMap()) {
            throw Exception("\"databases\" entry should be a map");
        }
        // database entry has keys datasets, runs, subruns, events, and products.
        std::vector<std::string> fields = { "datasets", "runs", "subruns", "events", "products" };
        for(auto& f : fields) {
            auto fieldNode = databasesNode[f];
            if(!fieldNode) {
                throw Exception("\""+f+"\" entry not found in databases section");
            }
            if(!fieldNode.IsMap()) {
                throw Exception("\""+f+"\" entry should be a mapping from addresses to providers");
            }
            for(auto addresses_it = fieldNode.begin(); addresses_it != fieldNode.end(); addresses_it++) {
                auto providers = addresses_it->second;
                for(auto provider_it = providers.begin(); provider_it != providers.end(); provider_it++) {
                    // provider entry should be a sequence
                    if(!provider_it->second.IsSequence()) {
                        throw Exception("provider entry should be a sequence");
                    }
                    for(auto db : provider_it->second) {
                        if(!db.IsScalar()) {
                            throw Exception("database id should be a scalar");
                        }
                    }
                }
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
        ch_placement_find_closest(m_databases.chi, name_hash, 1, &sdskv_db_idx);
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
        auto& db = m_databases.dbs[sdskv_db_idx];
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
        auto& db = m_databases.dbs[sdskv_db_idx];
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
        auto& db = m_databases.dbs[sdskv_db_idx];
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
        const auto& db = m_databases.dbs[sdskv_db_idx];
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
        const auto& db = m_databases.dbs[db_index];
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
        ch_placement_find_closest(m_databases.chi, h, 1, &db_idx);
        // make an entry for the lower bound
        auto lb_entry = buildKey(level, containerName, lower);
        // get provider and database
        const auto& db = m_databases.dbs[db_idx];
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
        std::vector<std::string> entries(maxKeys, std::string(1024,'\0'));
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

    ///////////////////////////////////////////////////////////////////////////
    // DataSet access functions
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Builds the database key of a particular DataSet.
     */
    static inline std::string _buildDataSetKey(uint8_t level, const std::string& containerName, const std::string& objectName) {
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

    /**
     * Locates and return the database in charge of the provided DataSet info.
     */
    const sdskv::database& _locateDataSetDb(const std::string& containerName) const {
        // hash the name to get the provider id
        long unsigned db_idx = 0;
        uint64_t hash;
        hash = hashString(containerName);
        ch_placement_find_closest(m_dataset_dbs.chi, hash, 1, &db_idx);
        return m_dataset_dbs.dbs[db_idx];
    }

    /**
     * @brief Fills the result vector with a sequence of up to
     * maxDataSets shared_ptr to DataSetImpl coming after the
     * current dataset. Returns the number of DataSets read.
     */
    size_t nextDataSets(const std::shared_ptr<DataSetImpl>& current, 
            std::vector<std::shared_ptr<DataSetImpl>>& result,
            size_t maxDataSets) const {
        int ret;
        result.resize(0);
        auto& level = current->m_level;
        auto& containerName = *current->m_container;
        auto& currentName = current->m_name;
        if(current->m_level == 0) return 0; // cannot iterate at object level
        auto& db = _locateDataSetDb(containerName);
        // make an entry for the lower bound
        auto lb_entry = _buildDataSetKey(level, containerName, currentName);
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
        std::vector<std::string> entries(maxDataSets, std::string(1024,'\0'));
        std::vector<UUID> uuids(maxDataSets);
        try {
            db.list_keyvals(lb_entry, prefix, entries, uuids);
        } catch(sdskv::exception& ex) {
            throw Exception("Error occured when calling sdskv::database::list_keys (SDSKV error="+std::string(ex.what()) + ")");
        }
        result.resize(0);
        for(const auto& entry : entries) {
            size_t i = entry.find_last_of('/');
            if(i == std::string::npos) i = 1;
            else i += 1;
            result.push_back(
                    std::make_shared<DataSetImpl>(
                        current->m_datastore,
                        level,
                        current->m_container,
                        entry.substr(i),
                        uuids[i]
                    )
                );
        }
        return result.size();
    }

    /**
     * @brief Checks if a particular dataset exists.
     */
    bool dataSetExists(uint8_t level, const std::string& containerName, const std::string& objectName) const {
        int ret;
        // build key
        auto key = buildKey(level, containerName, objectName);
        // find out which DB to access
        auto& db = _locateDataSetDb(containerName);
        try {
            bool b = db.exists(key);
            return b;
        } catch(sdskv::exception& ex) {
            throw Exception("Error occured when calling sdskv::database::exists (SDSKV error="+std::to_string(ex.error())+")");
        }
        return false;
    }
    
    /*
     * @brief Loads a dataset.
     */
    bool loadDataSet(uint8_t level, const std::string& containerName, const std::string& objectName, UUID& uuid) const {
        int ret;
        // build key
        auto key = buildKey(level, containerName, objectName);
        // find out which DB to access
        auto& db = _locateDataSetDb(containerName);
        try {
            size_t s = sizeof(uuid);
            db.get(static_cast<const void*>(key.data()), 
                   key.size(),
                   static_cast<void*>(uuid.data),
                   &s);
            return s == sizeof(uuid);
        } catch(sdskv::exception& ex) {
            if(ex.error() == SDSKV_ERR_UNKNOWN_KEY) {
                return false;
            }
            throw Exception("Error occured when calling sdskv::database::get (SDSKV error="+std::to_string(ex.error())+")");
        }
        return false;
    }

    /**
     * Creates a DataSet
     */
    bool createDataSet(uint8_t level, const std::string& containerName, const std::string& objectName, UUID& uuid) {
        // build full name
        auto key = _buildDataSetKey(level, containerName, objectName);
        // find out which DB to access
        auto& db = _locateDataSetDb(containerName);
        uuid.randomize();
        try {
            db.put(key.data(), key.size(), uuid.data, sizeof(uuid));
        } catch(sdskv::exception& ex) {
            if(!ex.error() == SDSKV_ERR_KEYEXISTS) {
                return false;
            } else {
                throw Exception("Error occured when calling sdskv::database::put (SDSKV error=" +std::to_string(ex.error()) + ")");
            }
        }
        return true;
    }
};

}

#endif
