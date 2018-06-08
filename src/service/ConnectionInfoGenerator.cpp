#include <fstream>
#include <yaml-cpp/yaml.h>
#include "ConnectionInfoGenerator.hpp"

namespace hepnos {

struct ConnectionInfoGenerator::Impl {
    std::string m_addr;      // address of this process
    uint16_t    m_bake_id;   // provider ids for BAKE
    uint16_t    m_sdskv_id;  // provider ids for SDSKV
};

ConnectionInfoGenerator::ConnectionInfoGenerator(
        const std::string& address, 
        uint16_t sdskv_provider_id,
        uint16_t bake_provider_id) 
: m_impl(std::make_unique<Impl>()) {
    m_impl->m_addr     = address;
    m_impl->m_bake_id  = bake_provider_id;
    m_impl->m_sdskv_id = sdskv_provider_id;
}

ConnectionInfoGenerator::~ConnectionInfoGenerator() {}

void ConnectionInfoGenerator::generateFile(MPI_Comm comm, const std::string& filename) const {
    int rank, size;
    const char* addr = m_impl->m_addr.c_str();

    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &size);

    unsigned j=0;
    while(addr[j] != '\0' && addr[j] != ':') j++;
    std::string proto(addr, j);

    // Exchange addresses
    std::vector<char> addresses_buf(128*size);
    MPI_Gather(addr, 128, MPI_BYTE, addresses_buf.data(), 128, MPI_BYTE, 0, comm);

    // Exchange bake providers info
    std::vector<uint16_t> bake_pr_ids_buf(size);
    MPI_Gather(&(m_impl->m_bake_id), 
            1, MPI_UNSIGNED_SHORT, 
            bake_pr_ids_buf.data(),
            1, MPI_UNSIGNED_SHORT, 
            0, comm);

    // Exchange sdskv providers info
    std::vector<uint16_t> sdskv_pr_ids_buf(size);
    MPI_Gather(&(m_impl->m_sdskv_id),
            1, MPI_UNSIGNED_SHORT, 
            sdskv_pr_ids_buf.data(),
            1, MPI_UNSIGNED_SHORT, 
            0, comm);

    // After this line, the rest is executed only by rank 0
    if(rank != 0) return;

    std::vector<std::string> addresses;
    for(unsigned i=0; i < size; i++) {
        addresses.emplace_back(&addresses_buf[128*i]);
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
