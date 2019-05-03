#ifndef __HEPNOS_SERVICE_CONFIG_H
#define __HEPNOS_SERVICE_CONFIG_H

#include <string>
#include <memory>
#include <yaml-cpp/yaml.h>

namespace hepnos {

class ServiceConfig {

private:
    class Impl;
    std::unique_ptr<Impl> m_impl;

public:

    ServiceConfig(const std::string& filename, int rank=0, int numRanks=1);
    
    ServiceConfig(const ServiceConfig&) = delete;
    ServiceConfig(ServiceConfig&&) = delete;
    ServiceConfig& operator=(const ServiceConfig&) = delete;
    ServiceConfig& operator=(ServiceConfig&&) = delete;
    ~ServiceConfig();

    const std::string& getAddress() const;
    bool hasDatabase() const;
    std::string getDatabasePath(int rank=0, int provider=0, int target=0) const;
    const std::string& getDatabasePathTemplate() const;
    std::string getDatabaseName(int rank=0, int provider=0, int target=0) const;
    const std::string& getDatabaseNameTemplate() const;
    const std::string& getDatabaseType() const;
    uint32_t getNumDatabaseProviders() const;
    uint32_t getNumDatabaseTargets() const;
    bool hasStorage() const;
    std::string getStoragePath(int rank=0, int provider=0, int target=0) const;
    const std::string& getStoragePathTemplate() const;
    size_t getStorageSize() const;
    uint32_t getNumStorageProviders() const;
    uint32_t getNumStorageTargets() const;
    uint32_t getNumThreads() const;
};

}

#endif
