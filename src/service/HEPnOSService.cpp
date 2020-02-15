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

static void createProviderAndDatabases(tl::engine& engine, hepnos::ProviderConfig& provider_config);

void hepnos_run_service(MPI_Comm comm, const char* config_file, const char* connection_file)
{
    int ret;
    int rank;
    int size;

    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &size);

    /* load configuration */
    std::unique_ptr<hepnos::ServiceConfig> config;
    try {
        config = std::make_unique<hepnos::ServiceConfig>(config_file, rank, size);
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
                    config->address,
                    THALLIUM_SERVER_MODE,
                    false, config->numThreads-1);

    } catch(std::exception& ex) {
        std::cerr << "Error: unable to initialize thallium" << std::endl;
        std::cerr << "Exception: " << ex.what() << std::endl;
        std::cerr << "Aborting." << std::endl;
        MPI_Abort(MPI_COMM_WORLD, -1);
        return;
    }
    engine->enable_remote_shutdown();
    auto self_addr_str  = static_cast<std::string>(engine->self());

    /* SDSKV providers initialization */
    for(auto& provider_config : config->datasetProviders)
        createProviderAndDatabases(*engine, provider_config);
    for(auto& provider_config : config->runProviders)
        createProviderAndDatabases(*engine, provider_config);
    for(auto& provider_config : config->subrunProviders)
        createProviderAndDatabases(*engine, provider_config);
    for(auto& provider_config : config->eventProviders)
        createProviderAndDatabases(*engine, provider_config);
    for(auto& provider_config : config->productProviders)
        createProviderAndDatabases(*engine, provider_config);

    hepnos::ConnectionInfoGenerator fileGen(self_addr_str, *config);
    fileGen.generateFile(MPI_COMM_WORLD, connection_file);

    engine->wait_for_finalize();
}

static void createProviderAndDatabases(tl::engine& engine, hepnos::ProviderConfig& provider_config) {

    sdskv::provider* provider = sdskv::provider::create(
            engine.get_margo_instance(), 
            provider_config.provider_id,
            SDSKV_ABT_POOL_DEFAULT);
    for(auto& db_config : provider_config.databases) {
        sdskv_config_t config;
        std::memset(&config, 0, sizeof(config));
        config.db_name = db_config.name.c_str();
        config.db_path = db_config.path.c_str();
        config.db_type = db_config.type;
        config.db_no_overwrite = 1;
        db_config.id = provider->attach_database(config);
    }
}
