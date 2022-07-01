/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRIVATE_DATASTORE_IMPL
#define __HEPNOS_PRIVATE_DATASTORE_IMPL

#include <vector>
#include <fstream>
#include <unordered_set>
#include <unordered_map>
#include <functional>
#include <iostream>
#include <nlohmann/json.hpp>
#include <thallium.hpp>
#include <yokan/cxx/database.hpp>
#include <yokan/cxx/client.hpp>
#include <ch-placement.h>
#include "hepnos/Exception.hpp"
#include "hepnos/DataStore.hpp"
#include "hepnos/DataSet.hpp"
#include "DatabaseAdaptor.hpp"
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
    std::vector<DatabaseAdaptor>  dbs;
    struct ch_placement_instance* chi = nullptr;
};

using nlohmann::json;
using namespace std::string_literals;
namespace tl = thallium;

class DataStoreImpl {
    public:

    tl::engine                                   m_engine;       // Thallium engine
    bool                                         m_engine_initialized = false;
    std::unordered_map<std::string,tl::endpoint> m_addrs;        // Addresses used by the service
    yokan::Client                                m_yokan_client; // Yokan client
    DistributedDBInfo                            m_dataset_dbs;  // list of Yokan databases for DataSets
    DistributedDBInfo                            m_run_dbs;      // list of Yokan databases for Runs
    DistributedDBInfo                            m_subrun_dbs;   // list of Yokan databases for SubRuns
    DistributedDBInfo                            m_event_dbs;    // list of Yokan databases for Events
    DistributedDBInfo                            m_product_dbs;  // list of Yokan databases for Products

    DataStoreImpl()
    {}

    ~DataStoreImpl() {
        cleanup();
    }

    void init(const std::string& protocol,
              const std::string& hepnosFile,
              const std::string& margoFile) {
        // Initializing thallium engine
        try {
            std::string config = "{}";
            if(!margoFile.empty()) {
                std::ifstream f(margoFile);
                if(!f.good()) {
                    throw Exception("Could not read margo config file "s + margoFile);
                }
                config = std::string((std::istreambuf_iterator<char>(f)),
                                      std::istreambuf_iterator<char>());
            }
            m_engine = tl::engine(protocol, THALLIUM_SERVER_MODE, config);
            m_engine_initialized = true;
        } catch(const std::exception& ex) {
            cleanup();
            throw Exception("Could not initialized Thallium: "s + ex.what());
        }
        // initialize Yokan client
        try {
            m_yokan_client = yokan::Client(m_engine.get_margo_instance());
        } catch(yokan::Exception& ex) {
            cleanup();
            throw Exception("Could not create Yokan client: "+std::string(ex.what()));
        }
        // parse hepnosFile
        json serviceConfig;
        {
            std::ifstream ifs(hepnosFile);
            serviceConfig = json::parse(ifs);
        }
        if (!serviceConfig.is_object()) {
            cleanup();
            throw Exception("Invalid JSON service configuration");
        }
        // Find databases in HEPnOS/Bedrock configuration file
        // Thanks to the hepnos-list-databases.jx9 script, the hepnos file has
        // the following json format:
        // { "address1" : {
        //       "datasets" : [ { "provider_id" : <id>, "database_id" : <id> }, ... ],
        //       "runs" : [ ... ],
        //       "subruns" : [ ... ],
        //       "events" : [ ... ],
        //       "products" : [ ... ]
        //   },
        //   "address2" : ...
        // }
        for (auto& entry : serviceConfig.items()) {
            auto& address = entry.key();
            // the other keys are valid addresses
            auto& nodeConfig = entry.value();
            // lookup the address
            tl::endpoint addr;
            if(m_addrs.count(address) == 0) {
                try {
                    addr = m_engine.lookup(address);
                    m_addrs[address] = addr;
                } catch(const std::exception& ex) {
                    cleanup();
                    throw Exception("Address lookup failed: "s + ex.what());
                }
            }

            auto populate_db_entries = [this, &addr](const json& cfg, DistributedDBInfo& db_info) {
                for(auto& entry : cfg) {
                    auto provider_id = entry["provider_id"].get<uint16_t>();
                    auto database_id_str = entry["database_id"].get<std::string>();
                    yk_database_id_t database_id;
                    yk_database_id_from_string(database_id_str.c_str(), &database_id);
                    db_info.dbs.push_back(
                        m_yokan_client.makeDatabaseHandle(addr.get_addr(), provider_id, database_id));
                }
            };

            populate_db_entries(nodeConfig["datasets"], m_dataset_dbs);
            populate_db_entries(nodeConfig["runs"],     m_run_dbs);
            populate_db_entries(nodeConfig["subruns"],  m_subrun_dbs);
            populate_db_entries(nodeConfig["events"],   m_event_dbs);
            populate_db_entries(nodeConfig["products"], m_product_dbs);
        }
        // Build ch-placement instances
        if (m_dataset_dbs.dbs.empty()) {
            cleanup();
            throw Exception("Could not find any database to store datasets");
        } else {
            m_dataset_dbs.chi = ch_placement_initialize("hash_lookup3", m_dataset_dbs.dbs.size(), 4, 0);
        }
        if (m_run_dbs.dbs.empty()) {
            cleanup();
            throw Exception("Could not find any database to store runs");
        } else {
            m_run_dbs.chi = ch_placement_initialize("hash_lookup3", m_run_dbs.dbs.size(), 4, 0);
        }
        if (m_subrun_dbs.dbs.empty()) {
            cleanup();
            throw Exception("Could not find any database to store subruns");
        } else {
            m_subrun_dbs.chi = ch_placement_initialize("hash_lookup3", m_subrun_dbs.dbs.size(), 4, 0);
        }
        if (m_event_dbs.dbs.empty()) {
            cleanup();
            throw Exception("Could not find any database to store events");
        } else {
            m_event_dbs.chi = ch_placement_initialize("hash_lookup3", m_event_dbs.dbs.size(), 4, 0);
        }
        if (m_product_dbs.dbs.empty()) {
            cleanup();
            throw Exception("Could not find any database to store products");
        } else {
            m_product_dbs.chi = ch_placement_initialize("hash_lookup3", m_product_dbs.dbs.size(), 4, 0);
        }
    }

    void cleanup() {
        m_dataset_dbs.dbs.clear();
        m_run_dbs.dbs.clear();
        m_subrun_dbs.dbs.clear();
        m_event_dbs.dbs.clear();
        m_product_dbs.dbs.clear();
        m_yokan_client = yokan::Client();
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

    const auto& locateProductDb(const ProductID& productID) const {
        return m_product_dbs.dbs[computeProductDbIndex(productID)];
    }

    const auto& getProductDatabase(unsigned long index) const {
        return m_product_dbs.dbs[index];
    }

    bool loadRawProduct(const ProductID& key,
                        std::string& data) const {
        // find out which DB to access
        auto& db =  locateProductDb(key);
        // read the value
        if(data.size() == 0)
            data.resize(8*1028); // eagerly allocate 8KB
        // TODO the above buffer size should be configurable
        // or I should implement a fetch() function in Yokan
        while(true) {
            try {
                size_t len = data.size();
                db.get(key.m_key.data(), key.m_key.size(),
                       const_cast<char*>(data.data()), &len);
                data.resize(len);
                break;
            } catch(yokan::Exception& ex) {
                if(ex.code() == YOKAN_ERR_KEY_NOT_FOUND)
                    return false;
                if(ex.code() == YOKAN_ERR_BUFFER_SIZE) {
                    size_t len = db.length(key.m_key.data(), key.m_key.size());
                    data.resize(len);
                    continue;
                }
                throw Exception("yokan::Database::get(): "+std::string(ex.what()));
            }
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
        } catch(yokan::Exception& ex) {
            if(ex.code() == YOKAN_ERR_KEY_NOT_FOUND)
                return false;
            else if(ex.code() == YOKAN_ERR_BUFFER_SIZE)
                return false;
            else
                throw Exception("yokan::Database::get(): "+std::string(ex.what()));
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
            db.put(key.m_key.data(), key.m_key.size(), value, vsize,
                   YOKAN_MODE_NEW_ONLY);
        } catch(yokan::Exception& ex) {
            if(ex.code() == YOKAN_ERR_KEY_EXISTS) {
                return ProductID();
            } else {
                throw Exception("yokan::Database::put(): " + std::string(ex.what()));
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
            db.put(key.m_key.data(), key.m_key.size(),
                   data.data(), data.size(),
                   YOKAN_MODE_NEW_ONLY);
        } catch(yokan::Exception& ex) {
            if(ex.code() == YOKAN_ERR_KEY_EXISTS) {
                return ProductID();
            } else {
                throw Exception("yokan::Database::put(): "+std::string(ex.what()));
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
        std::vector<char> buffer(10240);
        std::vector<size_t> ksizes(128);
        try {
            while(true) {
                const auto& start = result.empty() ? prefix : result.back();
                db.listKeysPacked(start.m_key.data(), start.m_key.size(),
                                  prefix.m_key.data(), prefix.m_key.size(),
                                  128, buffer.data(), buffer.size(), ksizes.data());
                size_t offset = 0;
                bool done = false;
                for(size_t i=0; i < ksizes.size(); i++) {
                    if(ksizes[i] == YOKAN_NO_MORE_KEYS) {
                        done = true;
                        break;
                    }
                    if(ksizes[i] == YOKAN_SIZE_TOO_SMALL) {
                        break;
                    }
                    result.emplace_back();
                    result.back().m_key.assign(buffer.data()+offset, ksizes[i]);
                    offset += ksizes[i];
                }
                if(done)
                    break;
            }
        } catch(yokan::Exception& ex) {
            throw Exception("yokan::Database::listKeys(): " + std::string(ex.what()));
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
    const auto& locateDataSetDb(const std::string& containerName) const {
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
        std::vector<char> packed_dataset_names(maxDataSets*1024);
        std::vector<size_t> packed_dataset_name_sizes(maxDataSets);
        std::vector<UUID> uuids(maxDataSets);
        std::vector<size_t> uuid_sizes(maxDataSets);
        try {
            db.listKeyValsPacked(lb_entry.data(), lb_entry.size(),
                                 prefix.data(), prefix.size(),
                                 maxDataSets,
                                 packed_dataset_names.data(),
                                 packed_dataset_names.size(),
                                 packed_dataset_name_sizes.data(),
                                 uuids.data(),
                                 uuids.size()*sizeof(UUID),
                                 uuid_sizes.data());
        } catch(yokan::Exception& ex) {
            throw Exception("yokan::Database::listKeys(): "+std::string(ex.what()));
        }
        result.resize(0);
        result.reserve(maxDataSets);
        size_t offset = 0;
        for(size_t i=0; i < maxDataSets; i++) {
            if(packed_dataset_name_sizes[i] > YOKAN_LAST_VALID_SIZE
            || uuid_sizes[i] > YOKAN_LAST_VALID_SIZE)
                return i;
            auto full_dset_name = std::string(packed_dataset_names.data()+offset, packed_dataset_name_sizes[i]);
            offset += packed_dataset_name_sizes[i];
            auto j = full_dset_name.find_last_of('/');
            if(j == std::string::npos) j =1;
            else j += 1;
            result.push_back(
                std::make_shared<DataSetImpl>(
                    current->m_datastore,
                    level,
                    current->m_container,
                    full_dset_name.substr(j),
                    uuids[i]
                )
            );
        }
        return maxDataSets;
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
            bool b = db.exists(key.data(), key.size(), YOKAN_MODE_NO_RDMA);
            return b;
        } catch(yokan::Exception& ex) {
            throw Exception("yokan::Database::exists(): "+std::string(ex.what()));
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
                   &s,
                   YOKAN_MODE_NO_RDMA);
            return s == sizeof(uuid);
        } catch(yokan::Exception& ex) {
            if(ex.code() == YOKAN_ERR_KEY_NOT_FOUND) {
                return false;
            }
            throw Exception("yokan::Database::get(): "+std::string(ex.what()));
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
            db.put(key.data(), key.size(), uuid.data, sizeof(uuid),
                   YOKAN_MODE_NEW_ONLY|YOKAN_MODE_NO_RDMA);
        } catch(yokan::Exception& ex) {
            if(ex.code() == YOKAN_ERR_KEY_EXISTS) {
                return false;
            } else {
                throw Exception("yokan::Database::put(): " +std::string(ex.what()));
            }
        }
        return true;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Access functions for numbered items (Runs, SubRuns, and Events)
    ///////////////////////////////////////////////////////////////////////////

    const auto& locateItemDb(const ItemType& type, const ItemDescriptor& id, int target=-1) const {
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
            hash = hash ^ id.run;
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
        const ItemDescriptor& start_key = current;
        auto& db = locateItemDb(item_type, start_key, target);
        descriptors.resize(maxItems);
        std::vector<size_t> ksizes(maxItems, sizeof(ItemDescriptor));
        size_t numItems = 0;
        try {
            db.listKeysPacked(&start_key, sizeof(start_key),
                         &start_key, ItemImpl::descriptorSize(prefix_type),
                         maxItems, descriptors.data(), descriptors.size()*sizeof(ItemDescriptor),
                         ksizes.data());
            for(numItems=0; numItems < maxItems; numItems++) {
                if(ksizes[numItems] > YOKAN_LAST_VALID_SIZE) break;
            }
        } catch(yokan::Exception& ex) {
            throw Exception("yokan::Database::listKeysPacked(): "+std::string(ex.what()));
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
            bool b = db.exists(&descriptor, sizeof(descriptor), YOKAN_MODE_NO_RDMA);
            return b;
        } catch(yokan::Exception& ex) {
            throw Exception("yokan::Database::exists(): "+std::string(ex.what()));
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
            db.put(&k, sizeof(k), nullptr, 0,
                   YOKAN_MODE_NEW_ONLY|YOKAN_MODE_NO_RDMA);
        } catch(yokan::Exception& ex) {
            if(ex.code() == YOKAN_ERR_KEY_EXISTS) {
                return false;
            } else {
                throw Exception("yokan::Database::put(): " +std::string(ex.what()));
            }
        }
        return true;
    }

};

}

#endif
