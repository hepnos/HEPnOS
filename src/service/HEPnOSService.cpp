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
#include <thallium.hpp>
#include <sdskv-server.hpp>
#include "ServiceConfig.hpp"
#include "ConnectionInfoGenerator.hpp"
#include "hepnos-service.h"

namespace tl = thallium;

#define ASSERT(__cond, __msg, ...) { if(!(__cond)) { fprintf(stderr, "[%s:%d] " __msg, __FILE__, __LINE__, __VA_ARGS__); exit(-1); } }

void hepnos_run_service(MPI_Comm comm, const char* config_file, const char* connection_file)
{
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

    std::unique_ptr<tl::engine> engine;
    try {
        engine = std::make_unique<tl::engine>(
                    config->getAddress(),
                    THALLIUM_SERVER_MODE,
                    false, config->getNumThreads()-1);

    } catch(std::exception& ex) {
        std::cerr << "Error: unable to initialize thallium" << std::endl;
        std::cerr << "Exception: " << ex.what() << std::endl;
        std::cerr << "Aborting." << std::endl;
        MPI_Abort(MPI_COMM_WORLD, -1);
        return;
    }
    engine->enable_remote_shutdown();
    auto self_addr_str  = static_cast<std::string>(engine->self());

    if(config->hasDatabase()) {
        /* SDSKV provider initialization */
        for(auto sdskv_provider_id = 0; sdskv_provider_id < config->getNumDatabaseProviders(); sdskv_provider_id++) {

            sdskv::provider* provider = sdskv::provider::create(
                    engine->get_margo_instance(), sdskv_provider_id, SDSKV_ABT_POOL_DEFAULT);

            for(unsigned i=0 ; i < config->getNumDatabaseTargets(); i++)  {
                auto db_path = config->getDatabasePath(rank, sdskv_provider_id, i);
                auto db_name = config->getDatabaseName(rank, sdskv_provider_id, i);
                sdskv_db_type_t db_type;
                if(config->getDatabaseType() == "null") db_type = KVDB_NULL;
                if(config->getDatabaseType() == "map") db_type = KVDB_MAP;
                if(config->getDatabaseType() == "ldb") db_type = KVDB_LEVELDB;
                if(config->getDatabaseType() == "bdb") db_type = KVDB_BERKELEYDB;
                sdskv_database_id_t db_id;
                sdskv_config_t config;
                std::memset(&config, 0, sizeof(config));
                config.db_name = db_name.c_str();
                config.db_path = db_path.c_str();
                config.db_type = db_type;
                config.db_no_overwrite = 1;
                db_id = provider->attach_database(config);
            }
        }
    }

    hepnos::ConnectionInfoGenerator fileGen(self_addr_str, 
            config->getNumDatabaseProviders(),
            config->getNumStorageProviders());
    fileGen.generateFile(MPI_COMM_WORLD, connection_file);

    engine->wait_for_finalize();
}
