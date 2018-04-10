#include <vector>
#include <functional>
#include <iostream>
#include <yaml-cpp/yaml.h>
#include <sdskv-client.h>
#include <bake-client.h>
#include <ch-placement.h>
#include "private/KeyTypes.hpp"
#include "hepnos/Exception.hpp"
#include "hepnos/DataStore.hpp"
#include "hepnos/DataSet.hpp"

namespace hepnos {

class DataStore::Impl {
    public:

        DataStore&                           m_parent;
        margo_instance_id                    m_mid;
        sdskv_client_t                       m_sdskv_client;
        bake_client_t                        m_bake_client;
        std::vector<sdskv_provider_handle_t> m_sdskv_ph;
        std::vector<sdskv_database_id_t>     m_sdskv_db;
        struct ch_placement_instance*        m_chi_sdskv;
        std::vector<bake_provider_handle_t>  m_bake_ph;
        struct ch_placement_instance*        m_chi_bake;
        const DataStore::iterator            m_end;

        Impl(DataStore& parent)
        : m_parent(parent)
        , m_mid(MARGO_INSTANCE_NULL)
        , m_sdskv_client(SDSKV_CLIENT_NULL)
        , m_chi_sdskv(nullptr)
        , m_bake_client(BAKE_CLIENT_NULL)
        , m_chi_bake(nullptr)
        , m_end(parent) {}

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
};

DataStore::DataStore(const std::string& configFile) 
: m_impl(std::make_unique<DataStore::Impl>(*this)) {
    m_impl->init(configFile);
}

DataStore::DataStore(DataStore&& other)
: m_impl(std::move(other.m_impl)) {}

DataStore& DataStore::operator=(DataStore&& other) {
    if(&other == this) return *this;
    if(m_impl) {
        m_impl->cleanup();
    }
    m_impl = std::move(other.m_impl);
}
    
DataStore::~DataStore() {
    if(m_impl) {
        m_impl->cleanup();
    }
}

DataStore::iterator DataStore::find(const std::string& datasetName) {
    // TODO
}

DataStore::const_iterator DataStore::find(const std::string& datasetName) const {
    // TODO
}

DataStore::iterator DataStore::begin() {
    return iterator(*this);
}

DataStore::iterator DataStore::end() {
    return m_impl->m_end;
}

DataStore::const_iterator DataStore::cbegin() const {
    return const_iterator(const_cast<DataStore&>(*this));
}

DataStore::const_iterator DataStore::cend() const {
    return m_impl->m_end;
}

DataStore::iterator DataStore::lower_bound(const std::string& lb) {
    // TODO
}

DataStore::const_iterator DataStore::lower_bound(const std::string& lb) const {
    // TODO
}

DataStore::iterator DataStore::upper_bound(const std::string& ub) {
    // TODO
}

DataStore::const_iterator DataStore::upper_bound(const std::string& ub) const {
    // TODO
}

DataSet DataStore::createDataSet(const std::string& name) {
    if(name.find('/') != std::string::npos) {
        throw Exception("Invalid character '/' in dataset name");
    }
    DataStoreEntryPtr entry = make_datastore_entry(0, name);
    // find which sdskv provider to contact
    uint64_t h = std::hash<std::string>()(name);
    unsigned long provider_idx;
    ch_placement_find_closest(m_impl->m_chi_sdskv, h, 1, &provider_idx);
    // store the key
    auto ph = m_impl->m_sdskv_ph[provider_idx];
    auto db_id = m_impl->m_sdskv_db[provider_idx];
    int ret = sdskv_put(ph, db_id, entry->raw(), entry->length(), NULL, 0);
    if(ret != SDSKV_SUCCESS) {
        throw Exception("Could not create DataSet (sdskv error)");
    }
    return DataSet(*this, 0, name);
}

DataStore::const_iterator::const_iterator(DataStore& ds)
: m_datastore(&ds) {
    // TODO
}

DataStore::const_iterator::~const_iterator() {
    // TODO
}

DataStore::const_iterator::const_iterator(const DataStore::const_iterator& other) 
: m_datastore(other.m_datastore) {
    // TODO
}

DataStore::const_iterator::const_iterator(DataStore::const_iterator&& other) 
: m_datastore(other.m_datastore) {
    // TODO
}

DataStore::const_iterator& DataStore::const_iterator::operator=(const DataStore::const_iterator&) {
    // TODO
}

DataStore::const_iterator& DataStore::const_iterator::operator=(DataStore::const_iterator&&) {
    // TODO
}

DataStore::const_iterator::self_type DataStore::const_iterator::operator++() {
    // TODO
}

DataStore::const_iterator::self_type DataStore::const_iterator::operator++(int) {
    // TODO
}

const DataStore::const_iterator::reference DataStore::const_iterator::operator*() {
    // TODO
}

const DataStore::const_iterator::pointer DataStore::const_iterator::operator->() {
    // TODO
}

bool DataStore::const_iterator::operator==(const self_type& rhs) const {
    // TODO
}

bool DataStore::const_iterator::operator!=(const self_type& rhs) const {
    // TODO
}

DataStore::iterator::iterator(DataStore& ds) 
: const_iterator(ds) {
    // TODO
}

DataStore::iterator::~iterator() {
    // TODO
}

DataStore::iterator::iterator(const DataStore::iterator& other)
: const_iterator(other) {
    // TODO
}

DataStore::iterator::iterator(DataStore::iterator&& other) 
: const_iterator(std::move(other)) {
    // TODO
}

DataStore::iterator& DataStore::iterator::operator=(const DataStore::iterator& other) {
    m_datastore = other.m_datastore;
    // TODO
}

DataStore::iterator& DataStore::iterator::operator=(DataStore::iterator&& other) {
    m_datastore = other.m_datastore;
}

DataStore::iterator::reference DataStore::iterator::operator*() {
    // TODO
}

DataStore::iterator::pointer DataStore::iterator::operator->() {
    // TODO
}

}

