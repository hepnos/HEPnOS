#ifndef __HEPNOS_CONNECTION_INFO_GENERATOR_H
#define __HEPNOS_CONNECTION_INFO_GENERATOR_H

#include <string>
#include <memory>
#include <mpi.h>
#include "ServiceConfig.hpp"

namespace hepnos {

class ConnectionInfoGenerator {

private:

    const std::string& address;
    const ServiceConfig& serviceConfig;

public:

    ConnectionInfoGenerator(const std::string& address,
                            const ServiceConfig& config);

    ConnectionInfoGenerator(const ConnectionInfoGenerator&) = delete;
    ConnectionInfoGenerator(ConnectionInfoGenerator&&) = delete;
    ConnectionInfoGenerator& operator=(const ConnectionInfoGenerator&) = delete;
    ConnectionInfoGenerator& operator=(ConnectionInfoGenerator&&) = delete;
    ~ConnectionInfoGenerator();

    void generateFile(MPI_Comm comm, const std::string& filename) const;
};

}

#endif
