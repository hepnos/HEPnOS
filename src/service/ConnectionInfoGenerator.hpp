#ifndef __HEPNOS_CONNECTION_INFO_GENERATOR_H
#define __HEPNOS_CONNECTION_INFO_GENERATOR_H

#include <string>
#include <memory>
#include <mpi.h>

namespace hepnos {

class ConnectionInfoGenerator {

private:

    class Impl;
    std::unique_ptr<Impl> m_impl;

public:

    ConnectionInfoGenerator(const std::string& address,
            uint16_t num_sdskv_providers, uint16_t num_bake_providers);
    ConnectionInfoGenerator(const ConnectionInfoGenerator&) = delete;
    ConnectionInfoGenerator(ConnectionInfoGenerator&&) = delete;
    ConnectionInfoGenerator& operator=(const ConnectionInfoGenerator&) = delete;
    ConnectionInfoGenerator& operator=(ConnectionInfoGenerator&&) = delete;
    ~ConnectionInfoGenerator();

    void generateFile(MPI_Comm comm, const std::string& filename) const;
};

}

#endif
