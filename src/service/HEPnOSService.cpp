/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <unistd.h>
#include <mpi.h>
#include <margo.h>
#include <bake-server.h>
#include <sdskv-server.h>
#include "ServiceConfig.hpp"
#include "ConnectionInfoGenerator.hpp"
#include "hepnos-service.h"

#define ASSERT(__cond, __msg, ...) { if(!(__cond)) { fprintf(stderr, "[%s:%d] " __msg, __FILE__, __LINE__, __VA_ARGS__); exit(-1); } }

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
    mid = margo_init(config->getAddress().c_str(), MARGO_SERVER_MODE, 0, config->getNumThreads()-1);
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
        for(auto bake_provider_id = 0; bake_provider_id < config->getNumStorageProviders(); bake_provider_id++) {
            /* create provider */
            bake_provider_t bake_prov;
            ret = bake_provider_register(mid, bake_provider_id, BAKE_ABT_POOL_DEFAULT, &bake_prov);
            ASSERT(ret == 0, "bake_provider_register() failed (ret = %d)\n", ret);
            /* create databases */
            for(unsigned i=0; i < config->getNumStorageTargets(); i++) {
                auto bake_target_name = config->getStoragePath(rank, bake_provider_id, i);
                size_t bake_target_size = config->getStorageSize()*(1024*1024);
                if(-1 == access(bake_target_name.c_str(), F_OK)) {
                    ret = bake_makepool(bake_target_name.c_str(), bake_target_size, 0664);
                    ASSERT(ret == 0, "bake_makepool() failed (ret = %d)\n", ret);
                }
                bake_target_id_t bake_tid;
                ret = bake_provider_add_storage_target(bake_prov, bake_target_name.c_str(), &bake_tid);
                ASSERT(ret == 0, "bake_provider_add_storage_target() failed to add target %s (ret = %d)\n",
                    bake_target_name.c_str(), ret);
            }
        }
    }

    if(config->hasDatabase()) {
        /* SDSKV provider initialization */
        for(auto sdskv_provider_id = 0; sdskv_provider_id < config->getNumDatabaseProviders(); sdskv_provider_id++) {
            sdskv_provider_t sdskv_prov;
            ret = sdskv_provider_register(mid, sdskv_provider_id, SDSKV_ABT_POOL_DEFAULT, &sdskv_prov);
            ASSERT(ret == 0, "sdskv_provider_register() failed (ret = %d)\n", ret);

            for(unsigned i=0 ; i < config->getNumDatabaseTargets(); i++)  {
                auto db_path = config->getDatabasePath(rank, sdskv_provider_id, i);
                auto db_name = config->getDatabaseName(rank, sdskv_provider_id, i);
                sdskv_db_type_t db_type;
                if(config->getDatabaseType() == "map") db_type = KVDB_MAP;
                if(config->getDatabaseType() == "ldb") db_type = KVDB_LEVELDB;
                if(config->getDatabaseType() == "bdb") db_type = KVDB_BERKELEYDB;
                sdskv_database_id_t db_id;
                sdskv_config_t config;
                std::memset(&config, 0, sizeof(config));
                config.db_name = db_name.c_str();
                config.db_path = db_path.c_str();
                config.db_type = db_type;
                ret = sdskv_provider_attach_database(sdskv_prov, &config,  &db_id);
                ASSERT(ret == 0, "sdskv_provider_attach_database() failed (ret = %d)\n", ret);
            }
        }
    }

    margo_addr_free(mid, self_addr);

    hepnos::ConnectionInfoGenerator fileGen(self_addr_str, 
            config->getNumDatabaseProviders(),
            config->getNumStorageProviders());
    fileGen.generateFile(MPI_COMM_WORLD, connection_file);

    margo_wait_for_finalize(mid);
}
