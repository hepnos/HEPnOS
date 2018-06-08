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

    ServiceConfig(const std::string& filename, int rank=-1);
    
    ServiceConfig(const ServiceConfig&) = delete;
    ServiceConfig(ServiceConfig&&) = delete;
    ServiceConfig& operator=(const ServiceConfig&) = delete;
    ServiceConfig& operator=(ServiceConfig&&) = delete;
    ~ServiceConfig();

    const std::string& getAddress() const;
    bool hasDatabase() const;
    const std::string& getDatabasePath() const;
    const std::string& getDatabaseName() const;
    const std::string& getDatabaseType() const;
    bool hasStorage() const;
    const std::string& getStoragePath() const;
    size_t getStorageSize() const;
};

}

#endif
