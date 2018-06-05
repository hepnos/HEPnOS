#include "ServiceConfig.hpp"
#include "hepnos/Exception.hpp"
#include <yaml-cpp/yaml.h>

namespace hepnos {

struct ServiceConfig::Impl {

    std::string m_address;
    bool        m_hasDatabase;
    std::string m_databasePath;
    std::string m_databaseName;
    std::string m_databaseType;
    bool        m_hasStorage;
    std::string m_storagePath;
    size_t      m_storageSize;

};

static YAML::Node loadAndValidate(const std::string& filename);
static std::string insertRankIn(const std::string& str, int rank);

ServiceConfig::ServiceConfig(const std::string& filename, int rank)
: m_impl(std::make_unique<Impl>()) {
    
    YAML::Node config       = loadAndValidate(filename);
    YAML::Node address      = config["address"];
    YAML::Node db_node      = config["database"];
    YAML::Node storage_node = config["storage"];
    m_impl->m_address = address.as<std::string>();
    if(!db_node) {
        m_impl->m_hasDatabase  = false;
    } else {
        m_impl->m_hasDatabase  = true;
        m_impl->m_databasePath = insertRankIn(db_node["path"].as<std::string>(), rank);
        m_impl->m_databaseName = db_node["name"].as<std::string>();
        m_impl->m_databaseType = db_node["type"].as<std::string>();
    }
    if(!storage_node) {
        m_impl->m_hasStorage   = false;
    } else {
        m_impl->m_hasStorage   = true;
        m_impl->m_storagePath  = insertRankIn(storage_node["path"].as<std::string>(), rank);
        m_impl->m_storageSize  = storage_node["size"].as<size_t>();
    }
}

ServiceConfig::~ServiceConfig() {}

const std::string& ServiceConfig::getAddress() const {
    return m_impl->m_address;
}

bool ServiceConfig::hasDatabase() const {
    return m_impl->m_hasDatabase;
}

const std::string& ServiceConfig::getDatabasePath() const {
    return m_impl->m_databasePath;
}

const std::string& ServiceConfig::getDatabaseName() const {
    return m_impl->m_databaseName;
}

const std::string& ServiceConfig::getDatabaseType() const {
    return m_impl->m_databaseType;
}

bool ServiceConfig::hasStorage() const {
    return m_impl->m_hasStorage;
}

const std::string& ServiceConfig::getStoragePath() const {
    return m_impl->m_storagePath;
}

size_t ServiceConfig::getStorageSize() const {
    return m_impl->m_storageSize;
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

static std::string insertRankIn(const std::string& str, int rank) {
    size_t index = 0;
    std::string result = str;
    std::stringstream ssrank;
    ssrank << rank;
    std::string srank = ssrank.str();
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
    return result;
}

}

