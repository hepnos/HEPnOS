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

void usage(void)
{
    fprintf(stderr, "Usage: hepnos-daemon <addr> <config>\n");
    fprintf(stderr, "  <config> path to the YAML file containing the service configuration\n");
    fprintf(stderr, "  <connection>  path to the YAML file to generate for clients\n");
    exit(-1);
}

int main(int argc, char *argv[])
{
    char* connection_file;
    char* config_file;
    int rank;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if(argc != 3) {
        if(rank == 0) {
            usage();
        }
        MPI_Finalize();
        exit(0);
    }

    config_file = argv[1];
    connection_file = argv[2];

    hepnos_run_service(MPI_COMM_WORLD, config_file, connection_file);

    MPI_Finalize();
}

