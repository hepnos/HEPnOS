#include <fstream>
#include <yaml-cpp/yaml.h>
#include "ConnectionInfoGenerator.hpp"

namespace hepnos {

struct ConnectionInfoGenerator::Impl {
    std::string m_addr;                // address of this process
    uint16_t    m_num_bake_providers;  // number of BAKE provider ids
    uint16_t    m_num_sdskv_providers; // number of SDSKV provider ids
};

ConnectionInfoGenerator::ConnectionInfoGenerator(
        const std::string& address, 
        uint16_t sdskv_providers,
        uint16_t bake_providers) 
: m_impl(std::make_unique<Impl>()) {
    m_impl->m_addr                 = address;
    m_impl->m_num_bake_providers   = bake_providers;
    m_impl->m_num_sdskv_providers  = sdskv_providers;
}

ConnectionInfoGenerator::~ConnectionInfoGenerator() {}

void ConnectionInfoGenerator::generateFile(MPI_Comm comm, const std::string& filename) const {
    int rank, size;
    auto addr_cpy = m_impl->m_addr;
    addr_cpy.resize(1024,'\0');

    const char* addr = addr_cpy.c_str();

    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &size);

    unsigned j=0;
    while(addr[j] != '\0' && addr[j] != ':' && addr[j] != ';') j++;
    std::string proto(addr, j);

    // Exchange addresses
    std::vector<char> addresses_buf(1024*size);
    MPI_Gather(addr, 1024, MPI_BYTE, addresses_buf.data(), 1024, MPI_BYTE, 0, comm);

    // Exchange bake providers info
    std::vector<uint16_t> bake_pr_ids_buf(size);
    MPI_Gather(&(m_impl->m_num_bake_providers), 
            1, MPI_UNSIGNED_SHORT, 
            bake_pr_ids_buf.data(),
            1, MPI_UNSIGNED_SHORT, 
            0, comm);

    // Exchange sdskv providers info
    std::vector<uint16_t> sdskv_pr_ids_buf(size);
    MPI_Gather(&(m_impl->m_num_sdskv_providers),
            1, MPI_UNSIGNED_SHORT, 
            sdskv_pr_ids_buf.data(),
            1, MPI_UNSIGNED_SHORT, 
            0, comm);

    // After this line, the rest is executed only by rank 0
    if(rank != 0) return;

    std::vector<std::string> addresses;
    for(unsigned i=0; i < size; i++) {
        addresses.emplace_back(&addresses_buf[1024*i]);
    }

    YAML::Node config;
    config["hepnos"]["client"]["protocol"] = proto;
    YAML::Node providers = config["hepnos"]["providers"];
    for(unsigned int i=0; i < size; i++) {
        const auto& provider_addr = addresses[i];
        if(sdskv_pr_ids_buf[i]) {
            providers["sdskv"][provider_addr] = sdskv_pr_ids_buf[i];
        }
        if(bake_pr_ids_buf[i]) {
            providers["bake"][provider_addr] = bake_pr_ids_buf[i];
        }
    }

    std::ofstream fout(filename);
    fout << config;
}
 
}
