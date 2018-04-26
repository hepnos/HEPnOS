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
#include "hepnos-service.h"

#define ASSERT(__cond, __msg, ...) { if(!(__cond)) { fprintf(stderr, "[%s:%d] " __msg, __FILE__, __LINE__, __VA_ARGS__); exit(-1); } }

static void generate_config_file(MPI_Comm comm, const char* addr, const char* config_file);

void hepnos_run_service(MPI_Comm comm, const char* listen_addr, const char* config_file)
{
    margo_instance_id mid;
    int ret;
    int rank;

    MPI_Comm_rank(comm, &rank);

    /* Margo initialization */
    mid = margo_init(listen_addr, MARGO_SERVER_MODE, 0, -1);
    if (mid == MARGO_INSTANCE_NULL)
    {
        fprintf(stderr, "Error: Unable to initialize margo\n");
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

    /* Bake provider initialization */
    uint16_t bake_mplex_id = 1;
    char bake_target_name[128];
    sprintf(bake_target_name, "/dev/shm/hepnos.%d.dat", rank);
    /* create the bake target if it does not exist */
    if(-1 == access(bake_target_name, F_OK)) {
        // XXX creating a pool of 10MB - this should come from a config file
        ret = bake_makepool(bake_target_name, 10*1024*1024, 0664);
        ASSERT(ret == 0, "bake_makepool() failed (ret = %d)\n", ret);
    }
    bake_provider_t bake_prov;
    bake_target_id_t bake_tid;
    ret = bake_provider_register(mid, bake_mplex_id, BAKE_ABT_POOL_DEFAULT, &bake_prov);
    ASSERT(ret == 0, "bake_provider_register() failed (ret = %d)\n", ret);
    ret = bake_provider_add_storage_target(bake_prov, bake_target_name, &bake_tid);
    ASSERT(ret == 0, "bake_provider_add_storage_target() failed to add target %s (ret = %d)\n",
            bake_target_name, ret);

    /* SDSKV provider initialization */
    uint8_t sdskv_mplex_id = 1;
    sdskv_provider_t sdskv_prov;
    ret = sdskv_provider_register(mid, sdskv_mplex_id, SDSKV_ABT_POOL_DEFAULT, &sdskv_prov);
    ASSERT(ret == 0, "sdskv_provider_register() failed (ret = %d)\n", ret);

    // XXX creating the database - this should come from a config file
    sdskv_database_id_t db_id;
    ret = sdskv_provider_add_database(sdskv_prov, "hepnosdb",  KVDB_MAP, SDSKV_COMPARE_DEFAULT,  &db_id);
    ASSERT(ret == 0, "sdskv_provider_add_database() failed (ret = %d)\n", ret);

    margo_addr_free(mid, self_addr);

    generate_config_file(MPI_COMM_WORLD, self_addr_str, config_file);

    margo_wait_for_finalize(mid);
}

static void generate_config_file(MPI_Comm comm, const char* addr, const char* config_file)
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

    std::ofstream fout(config_file);
    fout << config;
}