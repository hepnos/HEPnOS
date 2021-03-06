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
#include <thallium.hpp>
#include <yaml-cpp/yaml.h>
#include <sdskv-client.hpp>
#include <ch-placement.h>
#include "hepnos/Exception.hpp"
#include "hepnos/DataStore.hpp"
#include "hepnos/DataSet.hpp"
#include "StringHash.hpp"
#include "DataSetImpl.hpp"
#include "ItemImpl.hpp"

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

using namespace std::string_literals;
namespace tl = thallium;

class DataStoreImpl {
    public:

    tl::engine                                   m_engine;       // Thallium engine
    bool                                         m_engine_initialized = false;
    std::unordered_map<std::string,tl::endpoint> m_addrs;        // Addresses used by the service
    sdskv::client                                m_sdskv_client; // SDSKV client
    DistributedDBInfo                            m_dataset_dbs;  // list of SDSKV databases for DataSets
    DistributedDBInfo                            m_run_dbs;      // list of SDSKV databases for Runs
    DistributedDBInfo                            m_subrun_dbs;   // list of SDSKV databases for Runs
    DistributedDBInfo                            m_event_dbs;    // list of SDSKV databases for Runs
    DistributedDBInfo                            m_product_dbs;  // list of SDSKV databases for Products

    DataStoreImpl()
    {}

    ~DataStoreImpl() {
        cleanup();
    }

    void populateDatabases(DistributedDBInfo& db_info, const YAML::Node& db_config) {
        int ret;
        hg_return_t hret;
        for(YAML::const_iterator address_it = db_config.begin(); address_it != db_config.end(); address_it++) {
            std::string str_addr = address_it->first.as<std::string>();
            YAML::Node providers = address_it->second;
            // lookup the address
            tl::endpoint addr;
            if(m_addrs.count(str_addr) != 0) {
                addr = m_addrs[str_addr];
            } else {
                try {
                    addr = m_engine.lookup(str_addr);
                    m_addrs[str_addr] = addr;
                } catch(const std::exception& ex) {
                    throw Exception("Address lookup failed: "s + ex.what());
                }
            }
            // iterate over providers for this address
            for(YAML::const_iterator provider_it = providers.begin(); provider_it != providers.end(); provider_it++) {
                // get the provider id
                uint16_t provider_id = provider_it->first.as<uint16_t>();
                // create provider handle
                sdskv::provider_handle ph;
                try {
                    ph = sdskv::provider_handle(m_sdskv_client, addr.get_addr(), provider_id);
                } catch(const std::exception& ex) {
                    throw Exception("Could not create SDSKV provider handle: "s + ex.what());
                }
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

    void init(const std::string& configFile, bool use_progress_thread) {
        int ret;
        hg_return_t hret;
        YAML::Node config = YAML::LoadFile(configFile);
        checkConfig(config);
        // get protocol
        std::string proto = config["hepnos"]["client"]["protocol"].as<std::string>();
        // get busy spin
        bool busySpin = config["hepnos"]["client"]["busy-spin"].as<bool>();
        // initialize Margo
        hg_init_info hg_opt;
        memset(&hg_opt, 0, sizeof(hg_opt));
        if(busySpin)
            hg_opt.na_init_info.progress_mode = NA_NO_BLOCK;
        try {
            m_engine = tl::engine(proto, THALLIUM_SERVER_MODE, use_progress_thread, -1, &hg_opt);
            m_engine_initialized = true;
        } catch(const std::exception& ex) {
            cleanup();
            throw Exception("Could not initialized Thallium: "s + ex.what());
        }
        // initialize SDSKV client
        try {
            m_sdskv_client = sdskv::client(m_engine.get_margo_instance());
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
        populateDatabases(m_run_dbs, run_db);
        populateDatabases(m_subrun_dbs, subrun_db);
        populateDatabases(m_event_dbs, event_db);
        populateDatabases(m_product_dbs, product_db);
    }

    void cleanup() {
        m_dataset_dbs.dbs.clear();
        m_run_dbs.dbs.clear();
        m_subrun_dbs.dbs.clear();
        m_event_dbs.dbs.clear();
        m_product_dbs.dbs.clear();
        m_sdskv_client = sdskv::client();
        if(m_dataset_dbs.chi) ch_placement_finalize(m_dataset_dbs.chi);
        if(m_run_dbs.chi)     ch_placement_finalize(m_run_dbs.chi);
        if(m_subrun_dbs.chi)  ch_placement_finalize(m_subrun_dbs.chi);
        if(m_event_dbs.chi)   ch_placement_finalize(m_event_dbs.chi);
        if(m_product_dbs.chi) ch_placement_finalize(m_product_dbs.chi);
        m_addrs.clear();
        if(m_engine_initialized) m_engine.finalize();
        m_engine_initialized = false;
    }

    size_t numTargets(const ItemType& type) const {
        switch(type) {
            case ItemType::DATASET:
                return m_dataset_dbs.dbs.size();
            case ItemType::RUN:
                return m_run_dbs.dbs.size();
            case ItemType::SUBRUN:
                return m_subrun_dbs.dbs.size();
            case ItemType::EVENT:
                return m_event_dbs.dbs.size();
            case ItemType::PRODUCT:
                return m_product_dbs.dbs.size();
        }
        return 0;
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

    ///////////////////////////////////////////////////////////////////////////
    // Product access functions
    ///////////////////////////////////////////////////////////////////////////

    static inline ProductID buildProductID(const ItemDescriptor& id, const std::string& productName) {
        ProductID result;
        result.m_key.resize(sizeof(id)+productName.size());
        std::memcpy(const_cast<char*>(result.m_key.data()), &id, sizeof(id));
        std::memcpy(const_cast<char*>(result.m_key.data()+sizeof(id)), productName.data(), productName.size());
        return result;
    }

    long unsigned computeProductDbIndex(const ProductID& productID) const {
        // hash the name to get the provider id
        long unsigned db_idx = 0;
        uint64_t hash;
        // we are taking only the dataset+run+subrun part of the productID
        hash = hashString(productID.m_key.c_str(), SubRunDescriptorLength);
        ch_placement_find_closest(m_product_dbs.chi, hash, 1, &db_idx);
        return db_idx;
    }

    const sdskv::database& locateProductDb(const ProductID& productID) const {
        return m_product_dbs.dbs[computeProductDbIndex(productID)];
    }

    const sdskv::database& getProductDatabase(unsigned long index) const {
        return m_product_dbs.dbs[index];
    }

    bool loadRawProduct(const ProductID& key,
                        std::string& data) const {
        // find out which DB to access
        auto& db =  locateProductDb(key);
        // read the value
        if(data.size() == 0)
            data.resize(2048); // eagerly allocate 2KB
        try {
            db.get(key.m_key, data);
        } catch(sdskv::exception& ex) {
            if(ex.error() == SDSKV_ERR_UNKNOWN_KEY)
                return false;
            else
                throw Exception("Error occured when calling sdskv::database::get (SDSKV error="+std::to_string(ex.error())+")");
        }
        return true;
    }

    bool loadRawProduct(const ItemDescriptor& id,
                        const std::string& productName,
                        std::string& data) const {
        // build product id
        auto key = buildProductID(id, productName);
        return loadRawProduct(key, data);
    }

    bool loadRawProduct(const ProductID& key,
                        char* value, size_t* vsize) const {
        // find out which DB to access
        auto& db =  locateProductDb(key);
        try {
            db.get(key.m_key.data(), key.m_key.size(), value, vsize);
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

    bool loadRawProduct(const ItemDescriptor& id,
                        const std::string& productName,
                        char* value, size_t* vsize) const {
        // build product id
        auto key = buildProductID(id, productName);
        return loadRawProduct(key, value, vsize);
    }

    ProductID storeRawProduct(const ItemDescriptor& id,
                              const std::string& productName,
                              const char* value, size_t vsize) const {
        // build product id
        auto key = buildProductID(id, productName);
        // find out which DB to access
        auto& db =  locateProductDb(key);
        // read the value
        try {
            db.put(key.m_key.data(), key.m_key.size(), value, vsize);
        } catch(sdskv::exception& ex) {
            if(ex.error() == SDSKV_ERR_KEYEXISTS) {
                return ProductID();
            } else {
                throw Exception("Error occured when calling sdskv::database::put (SDSKV error=" +std::to_string(ex.error()) + ")");
            }
        }
        return key;
    }

    ProductID storeRawProduct(const ItemDescriptor& id,
                              const std::string& productName,
                              const std::string& data) const {
        // build product id
        auto key = buildProductID(id, productName);
        // find out which DB to access
        auto& db = locateProductDb(key);
        // store the value
        try {
            db.put(key.m_key, data);
        } catch(sdskv::exception& ex) {
            if(ex.error() == SDSKV_ERR_KEYEXISTS) {
                return ProductID();
            } else {
                throw Exception("Error occured when calling sdskv::database::put (SDSKV error=" +std::to_string(ex.error()) + ")");
            }
        }
        return key;
    }

    std::vector<ProductID> listProducts(const ItemDescriptor& id, const std::string& label) const {
        // make the prefix for the keys associated with the products
        // Note: normally buildProductID expects a product name (i.e. "label#type")
        // but by providing just the label, we have a product id representing a prefix
        auto prefix = buildProductID(id, label);
        // locate database containing products of interest
        auto& db = locateProductDb(prefix);
        std::vector<ProductID> result;
        result.reserve(8); // pretty arbitrary
        try {
            while(true) {
                std::vector<std::string> keys(8);
                const auto& start = result.empty() ? prefix : result.back();
                db.list_keys(start.m_key, prefix.m_key, keys);
                if(keys.empty())
                    break;
                for(auto& k : keys) {
                    result.emplace_back();
                    result.back().m_key = std::move(k);
                }
            }
        } catch(sdskv::exception& ex) {
            throw Exception("Error occured when calling sdskv::database::list_keys (SDSKV error=" + std::to_string(ex.error()) + ")");
        }
        return result;
    }

    ///////////////////////////////////////////////////////////////////////////
    // DataSet access functions
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Builds the database key of a particular DataSet.
     */
    static inline std::string buildDataSetKey(uint8_t level, const std::string& containerName, const std::string& objectName) {
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
    const sdskv::database& locateDataSetDb(const std::string& containerName) const {
        // hash the name to get the provider id
        long unsigned db_idx = 0;
        uint64_t hash;
        hash = hashString(containerName.c_str(), containerName.size());
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
        auto& db = locateDataSetDb(containerName);
        // make an entry for the lower bound
        auto lb_entry = buildDataSetKey(level, containerName, currentName);
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
        unsigned j=0;
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
                        uuids[j]
                    )
                );
            j += 1;
        }
        return result.size();
    }

    /**
     * @brief Checks if a particular dataset exists.
     */
    bool dataSetExists(uint8_t level, const std::string& containerName, const std::string& objectName) const {
        int ret;
        // build key
        auto key = buildDataSetKey(level, containerName, objectName);
        // find out which DB to access
        auto& db = locateDataSetDb(containerName);
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
        auto key = buildDataSetKey(level, containerName, objectName);
        // find out which DB to access
        auto& db = locateDataSetDb(containerName);
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
        auto key = buildDataSetKey(level, containerName, objectName);
        // find out which DB to access
        auto& db = locateDataSetDb(containerName);
        uuid.randomize();
        try {
            db.put(key.data(), key.size(), uuid.data, sizeof(uuid));
        } catch(sdskv::exception& ex) {
            if(ex.error() == SDSKV_ERR_KEYEXISTS) {
                return false;
            } else {
                throw Exception("Error occured when calling sdskv::database::put (SDSKV error=" +std::to_string(ex.error()) + ")");
            }
        }
        return true;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Access functions for numbered items (Runs, SubRuns, and Events)
    ///////////////////////////////////////////////////////////////////////////

    const sdskv::database& locateItemDb(const ItemType& type, const ItemDescriptor& id, int target=-1) const {
        long unsigned db_idx = 0;
        if(target >= 0) {
            if(type == ItemType::RUN)    return m_run_dbs.dbs[target];
            if(type == ItemType::SUBRUN) return m_subrun_dbs.dbs[target];
            if(type == ItemType::EVENT)  return m_event_dbs.dbs[target];
        }
        uint64_t hash;
        size_t prime = 1099511628211ULL;
        hash = id.dataset.hash();
        if(type == ItemType::RUN) { // we are locating a Run
            ch_placement_find_closest(m_run_dbs.chi, hash, 1, &db_idx);
            return m_run_dbs.dbs[db_idx];
        } else if(type == ItemType::SUBRUN) { // we are locating a SubRun
            hash *= prime;
            hash = hash ^ id.run;
            ch_placement_find_closest(m_subrun_dbs.chi, hash, 1, &db_idx);
            return m_subrun_dbs.dbs[db_idx];
        } else { // we are locating an Event
            hash *= prime;
            hash = hash ^ id.subrun;
            ch_placement_find_closest(m_event_dbs.chi, hash, 1, &db_idx);
            return m_event_dbs.dbs[db_idx];
        }
    }

    /**
     * @brief Fills the result vector with a sequence of up to
     * maxItems descriptors coming after provided current descriptor.
     */
    size_t nextItemDescriptors(
            const ItemType& item_type,
            const ItemType& prefix_type,
            const ItemDescriptor& current,
            std::vector<ItemDescriptor>& descriptors,
            size_t maxItems,
            int target=-1) const {
        int ret;
        const ItemDescriptor& start_key   = current;
        auto& db = locateItemDb(item_type, start_key, target);
        // ignore keys that don't have the same uuid
        // issue an sdskv_list_keys
        descriptors.resize(maxItems);
        std::vector<void*> keys_addr(maxItems);
        std::vector<hg_size_t> keys_sizes(maxItems, sizeof(ItemDescriptor));
        for(auto i=0; i < maxItems; i++) {
            keys_addr[i] = static_cast<void*>(&descriptors[i]);
        }
        size_t numItems = maxItems;
        try {
            db.list_keys(&start_key, sizeof(start_key),
                         &start_key, ItemImpl::descriptorSize(prefix_type),
                         keys_addr.data(), keys_sizes.data(), &numItems);
        } catch(sdskv::exception& ex) {
            throw Exception("Error occured when calling sdskv::database::list_keys (SDSKV error="+std::string(ex.what()) + ")");
        }
        descriptors.resize(numItems);
        return numItems;
    }

    /**
     * @brief Fills the result vector with a sequence of up to
     * maxRuns shared_ptr to RunImpl coming after the
     * current run. Returns the number of Runs read.
     */
    size_t nextItems(
            const ItemType& item_type,
            const ItemType& prefix_type,
            const std::shared_ptr<ItemImpl>& current,
            std::vector<std::shared_ptr<ItemImpl>>& result,
            size_t maxItems,
            int target=-1) const {
        const ItemDescriptor& start_key = current->m_descriptor;
        std::vector<ItemDescriptor> descriptors;
        size_t numDescriptors = nextItemDescriptors(item_type, prefix_type,
                start_key, descriptors, maxItems, target);
        descriptors.resize(numDescriptors);
        result.resize(0);
        result.reserve(numDescriptors);
        for(const auto& key : descriptors) {
            result.push_back(std::make_shared<ItemImpl>(current->m_datastore, key));
        }
        return result.size();
    }

    /**
     * @brief Checks if a particular Run/SubRun/Event exists.
     */
    bool itemExists(const ItemDescriptor& descriptor,
                    int target = -1) const {
        ItemType type = ItemType::RUN;
        if(descriptor.subrun != InvalidSubRunNumber) {
            type = ItemType::SUBRUN;
            if(descriptor.event != InvalidEventNumber)
                type = ItemType::EVENT;
        }
        // find out which DB to access
        auto& db = locateItemDb(type, descriptor, target);
        try {
            bool b = db.exists(&descriptor, sizeof(descriptor));
            return b;
        } catch(sdskv::exception& ex) {
            throw Exception("Error occured when calling sdskv::database::exists (SDSKV error="+std::to_string(ex.error())+")");
        }
        return false;
    }

    /**
     * @brief Checks if a particular Run/SubRun/Event exists.
     */
    bool itemExists(const UUID& containerUUID,
                    const RunNumber& run_number,
                    const SubRunNumber& subrun_number = InvalidSubRunNumber,
                    const EventNumber& event_number = InvalidEventNumber,
                    int target = -1) const {
        // build the key
        ItemDescriptor k;
        k.dataset = containerUUID;
        k.run     = run_number;
        k.subrun  = subrun_number;
        k.event   = event_number;
        return itemExists(k, target);
    }

    /**
     * Creates a Run, SubRun, or Event
     */
    bool createItem(const UUID& containerUUID,
                    const RunNumber& run_number,
                    const SubRunNumber& subrun_number = InvalidSubRunNumber,
                    const EventNumber& event_number = InvalidEventNumber) {
        // build the key
        ItemDescriptor k;
        k.dataset = containerUUID;
        k.run     = run_number;
        k.subrun  = subrun_number;
        k.event   = event_number;
        ItemType type = ItemType::RUN;
        if(subrun_number != InvalidSubRunNumber) {
            type = ItemType::SUBRUN;
            if(event_number != InvalidEventNumber)
                type = ItemType::EVENT;
        }
        // find out which DB to access
        auto& db = locateItemDb(type, k);
        try {
            db.put(&k, sizeof(k), nullptr, 0);
        } catch(sdskv::exception& ex) {
            if(ex.error() == SDSKV_ERR_KEYEXISTS) {
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
