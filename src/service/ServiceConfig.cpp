#include <cmath>
#include <iomanip>
#include "ServiceConfig.hpp"
#include "hepnos/Exception.hpp"
#include <yaml-cpp/yaml.h>

namespace hepnos {

struct ServiceConfig::Impl {

    std::string m_address;
    uint32_t    m_numRanks;
    bool        m_hasDatabase;
    std::string m_databasePath;
    std::string m_databaseName;
    std::string m_databaseType;
    uint32_t    m_databaseProviders = 1;
    uint32_t    m_databaseTargets = 1;
    bool        m_hasStorage;
    std::string m_storagePath;
    size_t      m_storageSize;
    uint32_t    m_storageProviders = 1;
    uint32_t    m_storageTargets = 1;
    uint32_t    m_numThreads = 1;
};

static YAML::Node loadAndValidate(const std::string& filename);
static std::string formatString(const std::string& str, 
        int rank, int provider, int target,
        int maxRank, int maxProvider, int maxTarget);

ServiceConfig::ServiceConfig(const std::string& filename, int rank, int numRanks)
: m_impl(std::make_unique<Impl>()) {
    
    m_impl->m_numRanks = numRanks;

    YAML::Node config       = loadAndValidate(filename);
    YAML::Node address      = config["address"];
    YAML::Node threads      = config["threads"];
    YAML::Node db_node      = config["database"];
    YAML::Node storage_node = config["storage"];
    if(threads) {
        m_impl->m_numThreads = threads.as<uint32_t>();
    }
    if(m_impl->m_numThreads == 0) m_impl->m_numThreads = 1;
    
    m_impl->m_address = address.as<std::string>();
    if(!db_node) {
        m_impl->m_hasDatabase  = false;
    } else {
        m_impl->m_hasDatabase  = true;
        m_impl->m_databasePath = db_node["path"].as<std::string>();
        m_impl->m_databaseName = db_node["name"].as<std::string>();
        m_impl->m_databaseType = db_node["type"].as<std::string>();
        if(db_node["providers"]) {
            m_impl->m_databaseProviders = db_node["providers"].as<uint32_t>();
        }
        if(db_node["targets"]) {
            m_impl->m_databaseTargets = db_node["targets"].as<uint32_t>();
        }
    }
    if(!storage_node) {
        m_impl->m_hasStorage   = false;
    } else {
        m_impl->m_hasStorage   = true;
        m_impl->m_storagePath  = storage_node["path"].as<std::string>();
        m_impl->m_storageSize  = storage_node["size"].as<size_t>();
        if(storage_node["providers"]) {
            m_impl->m_storageProviders = storage_node["providers"].as<uint32_t>();
        }
        if(storage_node["targets"]) {
            m_impl->m_storageTargets = storage_node["targets"].as<uint32_t>();
        }
    }
}

ServiceConfig::~ServiceConfig() {}

const std::string& ServiceConfig::getAddress() const {
    return m_impl->m_address;
}

bool ServiceConfig::hasDatabase() const {
    return m_impl->m_hasDatabase;
}

std::string ServiceConfig::getDatabasePath(int rank, int provider, int target) const {
    int maxRank     = m_impl->m_numRanks - 1;
    int maxProvider = getNumDatabaseProviders() - 1;
    int maxTarget   = getNumDatabaseTargets() - 1;
    return formatString(m_impl->m_databasePath, rank, provider, target, maxRank, maxProvider, maxTarget);
}

std::string ServiceConfig::getDatabaseName(int rank, int provider, int target) const {
    int maxRank     = m_impl->m_numRanks - 1;
    int maxProvider = getNumDatabaseProviders() - 1;
    int maxTarget   = getNumDatabaseTargets() - 1;
    return formatString(m_impl->m_databaseName, rank, provider, target, maxRank, maxProvider, maxTarget);
}

const std::string& ServiceConfig::getDatabasePathTemplate() const {
    return m_impl->m_databasePath;
}

const std::string& ServiceConfig::getDatabaseNameTemplate() const {
    return m_impl->m_databaseName;
}

const std::string& ServiceConfig::getDatabaseType() const {
    return m_impl->m_databaseType;
}

uint32_t ServiceConfig::getNumDatabaseProviders() const {
    return m_impl->m_databaseProviders;
}

uint32_t ServiceConfig::getNumDatabaseTargets() const {
    return m_impl->m_databaseTargets;
}

bool ServiceConfig::hasStorage() const {
    return m_impl->m_hasStorage;
}

std::string ServiceConfig::getStoragePath(int rank, int provider, int target) const {
    int maxRank     = m_impl->m_numRanks - 1;
    int maxProvider = getNumStorageProviders() - 1;
    int maxTarget   = getNumStorageTargets() - 1;
    return formatString(m_impl->m_storagePath, rank, provider, target, maxRank, maxProvider, maxTarget);
}

const std::string& ServiceConfig::getStoragePathTemplate() const {
    return m_impl->m_storagePath;
}

size_t ServiceConfig::getStorageSize() const {
    return m_impl->m_storageSize;
}

uint32_t ServiceConfig::getNumStorageProviders() const {
    return m_impl->m_storageProviders;
}

uint32_t ServiceConfig::getNumStorageTargets() const {
    return m_impl->m_storageTargets;
}

uint32_t ServiceConfig::getNumThreads() const {
    return m_impl->m_numThreads;
}

static YAML::Node loadAndValidate(const std::string& filename) {
    YAML::Node config = YAML::LoadFile(filename);
    if(!config["address"]) {
        throw Exception("\"address\" field not found in configuration file.");
    }
    if(!config["database"]) {
        throw Exception("\"database\" field not found in configuration file.");
    }
    if(!config["database"]["path"]) {
        throw Exception("\"database.path\" field not found in configuration file.");
    }
    if(!config["database"]["name"]) {
        throw Exception("\"database.name\" field not found in configuration file.");
    }
    if(!config["database"]["type"]) {
        throw Exception("\"database.type\" field not found in configuration file.");
    }
    std::string db_type = config["database"]["type"].as<std::string>();
    if(db_type != "map"
    && db_type != "ldb"
    && db_type != "bdb") {
        throw Exception("\"database.type\" field should be \"map\", \"ldb\", or \"bdb\".");
    }
    if(config["storage"]) {
        if(!config["storage"]["path"]) {
            throw Exception("\"storage.path\" field not found in configuration file.");
        }
        if(!config["storage"]["size"]) {
            throw Exception("\"storage.size\" field not found in configuration file.");
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

