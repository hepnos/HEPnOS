#include <cmath>
#include <iomanip>
#include <iostream>
#include <unordered_map>
#include "ServiceConfig.hpp"
#include "hepnos/Exception.hpp"
#include <yaml-cpp/yaml.h>

namespace hepnos {

static YAML::Node loadAndValidate(const std::string& filename);

static std::string formatString(const std::string& str, 
        int rank, int provider, int target,
        int maxRank, int maxProvider, int maxTarget);

static std::vector<ProviderConfig> parseDatabaseEntry(
        const YAML::Node& entry, int rank, int maxRank,
        uint16_t start_provider_id,
        int maxProviders);

ServiceConfig::ServiceConfig(const std::string& filename, int rank, int numRanks)
{
    
    this->numRanks = numRanks;

    YAML::Node config    = loadAndValidate(filename);
    YAML::Node address   = config["address"];
    YAML::Node threads   = config["threads"];
    YAML::Node databases = config["databases"];
    YAML::Node busySpin  = config["busy-spin"];
    if(busySpin) this->busySpin = busySpin.as<bool>();
    if(threads) this->numThreads = threads.as<uint32_t>();
    this->address = address.as<std::string>();
    // Count the total number of providers and targets
    unsigned maxProviders = 0;
    std::vector<std::string> fields = {"datasets", "runs", "subruns", "events", "products"};
    for(const auto& field : fields) {
        uint32_t num_providers = 1;
        if(databases[field]["providers"]) num_providers = databases[field]["providers"].as<uint32_t>();
        maxProviders += num_providers;
    }
    // DataSet DB
    uint16_t start_provider_id = 0;
    this->datasetProviders = parseDatabaseEntry(databases["datasets"], rank, numRanks, start_provider_id, maxProviders);
    start_provider_id += this->datasetProviders.size();
    // Run DB
    this->runProviders = parseDatabaseEntry(databases["runs"], rank, numRanks, start_provider_id, maxProviders);
    start_provider_id += this->runProviders.size();
    // SubRun DB
    this->subrunProviders = parseDatabaseEntry(databases["subruns"], rank, numRanks, start_provider_id, maxProviders);
    start_provider_id += this->subrunProviders.size();
    // Event DB
    this->eventProviders = parseDatabaseEntry(databases["events"], rank, numRanks, start_provider_id, maxProviders);
    start_provider_id += this->eventProviders.size();
    // Product DB
    this->productProviders = parseDatabaseEntry(databases["products"], rank, numRanks, start_provider_id, maxProviders);
}

ServiceConfig::~ServiceConfig() {}

static std::vector<ProviderConfig> parseDatabaseEntry(
        const YAML::Node& db, int rank, int maxRanks,
        uint16_t start_provider_id, int maxProviders)
{
    std::unordered_map<std::string, sdskv_db_type_t> sdskv_type_from;
    sdskv_type_from["null"] = KVDB_NULL;
    sdskv_type_from["map"]  = KVDB_MAP;
    sdskv_type_from["ldb"]  = KVDB_LEVELDB;
    sdskv_type_from["bdb"]  = KVDB_BERKELEYDB;

    uint16_t provider_id = start_provider_id;
    auto nameTemplate      = db["name"].as<std::string>();
    auto pathTemplate      = db["path"].as<std::string>();
    auto type              = db["type"].as<std::string>();
    uint32_t num_providers = 1;
    uint32_t num_targets   = 1;
    if(db["providers"]) num_providers = db["providers"].as<uint32_t>();
    if(db["targets"])   num_targets   = db["targets"].as<uint32_t>();
    // populate provider and database config
    std::vector<ProviderConfig> result;
    for(unsigned i = 0; i < num_providers; i++) {
        ProviderConfig pconfig;
        pconfig.provider_id = provider_id;
        for(unsigned j = 0; j < num_targets; j++) {
            DataBaseConfig dbconfig;
            dbconfig.name = formatString(nameTemplate, rank, provider_id, j,
                    maxRanks, maxProviders, num_targets);
            dbconfig.path = formatString(pathTemplate, rank, provider_id, j,
                    maxRanks, maxProviders, num_targets);
            dbconfig.type = sdskv_type_from[type];
            pconfig.databases.push_back(dbconfig);
        }
        provider_id += 1;
        result.push_back(pconfig);
    }
    return result;
}

// Configuration file validation
static YAML::Node loadAndValidate(const std::string& filename) {
    YAML::Node config = YAML::LoadFile(filename);
    if(!config["address"]) {
        throw Exception("\"address\" field not found in configuration file.");
    }
    if(!config["databases"]) {
        throw Exception("\"database\" field not found in configuration file.");
    }
    std::vector<std::string> db_fields = { "datasets", "runs", "subruns", "events", "products" };
    for(auto& f : db_fields) {
        if(!config["databases"][f]) {
            throw Exception("\"databases."+f+"\" not found in configuration file.");
        }
        auto db_config = config["databases"][f];
        if(!db_config["path"]) {
            throw Exception("\"databases."+f+".path\" field not found in configuration file.");
        }
        if(!db_config["name"]) {
            throw Exception("\"databases."+f+".name\" field not found in configuration file.");
        }
        if(!db_config["type"]) {
            throw Exception("\"databases."+f+".type\" field not found in configuration file.");
        }
        std::string db_type = db_config["type"].as<std::string>();
        if(db_type != "null"
        && db_type != "map"
        && db_type != "ldb"
        && db_type != "bdb") {
            throw Exception("\"databases"+f+".type\" field should be \"null\", \"map\", \"ldb\", or \"bdb\".");
        }
    }
    return config;
}

static std::string formatString(const std::string& str, 
        int rank, int provider, int target,
        int maxRank=0, int maxProvider=0, int maxTarget=0) {
    std::string result = str;
    std::stringstream ssrank;
    ssrank << std::setw(log10(maxRank+1)+1) << std::setfill('0') << rank;
    std::stringstream ssprovider;
    ssprovider << std::setw(log10(maxProvider+1)+1) << std::setfill('0') << provider;
    std::stringstream sstarget;
    sstarget << std::setw(log10(maxTarget+1)+1) << std::setfill('0') << target;
    std::string srank = ssrank.str();
    std::string sprovider = ssprovider.str();
    std::string starget = sstarget.str();
    size_t index = 0;
    while (true) {
        index = result.find("$RANK", index);
        if (index == std::string::npos) break;
        if(rank >= 0) {
            result.replace(index, 5, srank.c_str());
        } else {
            result.replace(index, 5, "");
        }
        index += 5;
    }
    index = 0;
    while (true) {
        index = result.find("$PROVIDER", index);
        if (index == std::string::npos) break;
        if(rank >= 0) {
            result.replace(index, 9, sprovider.c_str());
        } else {
            result.replace(index, 9, "");
        }
        index += 9;
    }
    index = 0;
    while (true) {
        index = result.find("$TARGET", index);
        if (index == std::string::npos) break;
        if(rank >= 0) {
            result.replace(index, 7, starget.c_str());
        } else {
            result.replace(index, 7, "");
        }
        index += 7;
    }
    return result;
}

}

