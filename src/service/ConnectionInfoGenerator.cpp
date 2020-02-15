#include <fstream>
#include <yaml-cpp/yaml.h>
#include "ConnectionInfoGenerator.hpp"

namespace hepnos {

ConnectionInfoGenerator::ConnectionInfoGenerator(
        const std::string& address,
        const ServiceConfig& config)
: address(address), serviceConfig(config) {}

ConnectionInfoGenerator::~ConnectionInfoGenerator() {}

void ConnectionInfoGenerator::generateFile(MPI_Comm comm, const std::string& filename) const {
    int rank, size;
    auto addr_cpy = address;
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

    std::vector<sdskv_database_id_t> local_ids;
    std::vector<sdskv_database_id_t> all_db_ids;
    for(auto& p : serviceConfig.datasetProviders)
        for(auto& db : p.databases)
            local_ids.push_back(db.id);
    for(auto& p : serviceConfig.runProviders)
        for(auto& db : p.databases)
            local_ids.push_back(db.id);
    for(auto& p : serviceConfig.subrunProviders)
        for(auto& db : p.databases)
            local_ids.push_back(db.id);
    for(auto& p : serviceConfig.eventProviders)
        for(auto& db : p.databases)
            local_ids.push_back(db.id);
    for(auto& p : serviceConfig.productProviders)
        for(auto& db : p.databases)
            local_ids.push_back(db.id);
    // Exchange database ids
    all_db_ids.resize(local_ids.size()*size);
    MPI_Gather(local_ids.data(), sizeof(sdskv_database_id_t)*local_ids.size(), MPI_BYTE,
               all_db_ids.data(), sizeof(sdskv_database_id_t)*local_ids.size(), MPI_BYTE, 0, comm);


    // After this line, the rest is executed only by rank 0
    if(rank != 0) return;

    std::vector<std::string> addresses;
    for(unsigned i=0; i < size; i++) {
        addresses.emplace_back(&addresses_buf[1024*i]);
    }

    YAML::Node config;
    config["hepnos"]["client"]["protocol"] = proto;
    YAML::Node databases = config["hepnos"]["databases"];
    YAML::Node datasets  = databases["datasets"];
    YAML::Node runs      = databases["runs"];
    YAML::Node subruns   = databases["subruns"];
    YAML::Node events    = databases["events"];
    YAML::Node products  = databases["products"];
    
    // for all the server nodes...
    for(unsigned i=0; i < size; i++) {
        YAML::Node datasetProviders = datasets[addresses[i]];
        YAML::Node runProviders     = runs[addresses[i]];
        YAML::Node subrunProviders  = subruns[addresses[i]];
        YAML::Node eventProviders   = events[addresses[i]];
        YAML::Node productProviders = products[addresses[i]];
        int db_per_node = local_ids.size();
        int j=0;
        for(auto& p : serviceConfig.datasetProviders) {
            auto provider = datasetProviders[std::to_string(p.provider_id)];
            for(auto& db : p.databases) {
                auto id = all_db_ids[i*db_per_node+j];
                provider.push_back(id);
                j += 1;
            }
        }  
        for(auto& p : serviceConfig.runProviders) {
            auto provider = runProviders[std::to_string(p.provider_id)];
            for(auto& db : p.databases) {
                auto id = all_db_ids[i*db_per_node+j];
                provider.push_back(id);
                j += 1;
            }
        }
        for(auto& p : serviceConfig.subrunProviders) {
            auto provider = subrunProviders[std::to_string(p.provider_id)];
            for(auto& db : p.databases) {
                auto id = all_db_ids[i*db_per_node+j];
                provider.push_back(id);
                j += 1;
            }
        }
        for(auto& p : serviceConfig.eventProviders) {
            auto provider = eventProviders[std::to_string(p.provider_id)];
            for(auto& db : p.databases) {
                auto id = all_db_ids[i*db_per_node+j];
                provider.push_back(id);
                j += 1;
            }
        }
        for(auto& p : serviceConfig.productProviders) {
            auto provider = productProviders[std::to_string(p.provider_id)];
            for(auto& db : p.databases) {
                auto id = all_db_ids[i*db_per_node+j];
                provider.push_back(id);
                j += 1;
            }
        }
    }

    std::ofstream fout(filename);
    fout << config;
}
 
}
