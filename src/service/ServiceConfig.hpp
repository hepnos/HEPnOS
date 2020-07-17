#ifndef __HEPNOS_SERVICE_CONFIG_H
#define __HEPNOS_SERVICE_CONFIG_H

#include <string>
#include <memory>
#include <sdskv-common.h>
#include <yaml-cpp/yaml.h>

namespace hepnos {

struct DataBaseConfig {
    std::string name;
    std::string path;
    sdskv_db_type_t type;
    sdskv_database_id_t id = 0;
};

struct ProviderConfig {
    uint16_t                    provider_id;
    std::vector<DataBaseConfig> databases;
};

class ServiceConfig {

public:

    std::string address;
    uint32_t    numRanks = 1;
    uint32_t    numThreads = 1;
    bool        busySpin = false;
    std::vector<ProviderConfig> datasetProviders;
    std::vector<ProviderConfig> runProviders;
    std::vector<ProviderConfig> subrunProviders;
    std::vector<ProviderConfig> eventProviders;
    std::vector<ProviderConfig> productProviders;

    ServiceConfig(const std::string& filename, int rank=0, int numRanks=1);
    
    ServiceConfig(const ServiceConfig&) = delete;
    ServiceConfig(ServiceConfig&&) = delete;
    ServiceConfig& operator=(const ServiceConfig&) = delete;
    ServiceConfig& operator=(ServiceConfig&&) = delete;
    ~ServiceConfig();

};

}

#endif
