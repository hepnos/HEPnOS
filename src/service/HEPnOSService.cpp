/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <unistd.h>
#include <mpi.h>
#include <margo.h>
#include <bake-server.h>
#include <sdskv-server.h>
#include <yaml-cpp/yaml.h>
#include "ServiceConfig.hpp"
#include "hepnos-service.h"

#define ASSERT(__cond, __msg, ...) { if(!(__cond)) { fprintf(stderr, "[%s:%d] " __msg, __FILE__, __LINE__, __VA_ARGS__); exit(-1); } }

static void generate_connection_file(MPI_Comm comm, const char* addr, const char* filename);

void hepnos_run_service(MPI_Comm comm, const char* config_file, const char* connection_file)
{
    margo_instance_id mid;
    int ret;
    int rank;

    MPI_Comm_rank(comm, &rank);

    /* load configuration */
    std::unique_ptr<hepnos::ServiceConfig> config;
    try {
        config = std::make_unique<hepnos::ServiceConfig>(config_file, rank);
    } catch(const std::exception& e) {
        std::cerr << "Error: when reading configuration:" << std::endl;
        std::cerr << "  " << e.what() << std::endl;
        std::cerr << "Aborting." << std::endl;
        MPI_Abort(MPI_COMM_WORLD, -1);
        return;
    }

    /* Margo initialization */
    mid = margo_init(config->getAddress().c_str(), MARGO_SERVER_MODE, 0, -1);
    if (mid == MARGO_INSTANCE_NULL)
    {
        std::cerr << "Error: unable to initialize margo" << std::endl;
        std::cerr << "Aborting." << std::endl;
        MPI_Abort(MPI_COMM_WORLD, -1);
        return;
    }
    margo_enable_remote_shutdown(mid);

    /* Get self address as string */
    hg_addr_t self_addr;
    margo_addr_self(mid, &self_addr);
    char self_addr_str[128];
    hg_size_t self_addr_str_size = 128;
    margo_addr_to_string(mid, self_addr_str, &self_addr_str_size, self_addr);

    if(config->hasStorage()) {
        /* Bake provider initialization */
        uint16_t bake_mplex_id = 1;
        const char* bake_target_name = config->getStoragePath().c_str();
        size_t bake_target_size = config->getStorageSize()*(1024*1024);
        /* create the bake target if it does not exist */
        if(-1 == access(bake_target_name, F_OK)) {
            ret = bake_makepool(bake_target_name, bake_target_size, 0664);
            ASSERT(ret == 0, "bake_makepool() failed (ret = %d)\n", ret);
        }
        bake_provider_t bake_prov;
        bake_target_id_t bake_tid;
        ret = bake_provider_register(mid, bake_mplex_id, BAKE_ABT_POOL_DEFAULT, &bake_prov);
        ASSERT(ret == 0, "bake_provider_register() failed (ret = %d)\n", ret);
        ret = bake_provider_add_storage_target(bake_prov, bake_target_name, &bake_tid);
        ASSERT(ret == 0, "bake_provider_add_storage_target() failed to add target %s (ret = %d)\n",
                bake_target_name, ret);
    }

    if(config->hasDatabase()) {
        /* SDSKV provider initialization */
        uint8_t sdskv_mplex_id = 1;
        sdskv_provider_t sdskv_prov;
        ret = sdskv_provider_register(mid, sdskv_mplex_id, SDSKV_ABT_POOL_DEFAULT, &sdskv_prov);
        ASSERT(ret == 0, "sdskv_provider_register() failed (ret = %d)\n", ret);

        /* creating the database */
        const char* db_path = config->getDatabasePath().c_str();
        const char* db_name = config->getDatabaseName().c_str();
        sdskv_db_type_t db_type;
        if(config->getDatabaseType() == "map") db_type = KVDB_MAP;
        if(config->getDatabaseType() == "ldb") db_type = KVDB_LEVELDB;
        if(config->getDatabaseType() == "bdb") db_type = KVDB_BERKELEYDB;
        sdskv_database_id_t db_id;
        ret = sdskv_provider_add_database(sdskv_prov, db_name, db_path, db_type, SDSKV_COMPARE_DEFAULT,  &db_id);
        ASSERT(ret == 0, "sdskv_provider_add_database() failed (ret = %d)\n", ret);
    }

    margo_addr_free(mid, self_addr);

    // XXX This should be replaced by submitting the connection info to a registry
    generate_connection_file(MPI_COMM_WORLD, self_addr_str, connection_file);

    margo_wait_for_finalize(mid);
}

static void generate_connection_file(MPI_Comm comm, const char* addr, const char* filename)
{
    int rank, size;
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &size);

    unsigned j=0;
    while(addr[j] != '\0' && addr[j] != ':') j++;
    std::string proto(addr, j);

    std::vector<char> buf(128*size);

    MPI_Gather(addr, 128, MPI_BYTE, buf.data(), 128, MPI_BYTE, 0, comm);
    
    if(rank != 0) return;

    std::vector<std::string> addresses;
    for(unsigned i=0; i < size; i++) {
        addresses.emplace_back(&buf[128*i]);
    }

    YAML::Node config;
    config["hepnos"]["client"]["protocol"] = proto;
    for(auto& s :  addresses)
        config["hepnos"]["providers"]["sdskv"][s] = 1;

    std::ofstream fout(filename);
    fout << config;
}
