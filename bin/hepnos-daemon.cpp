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
#include "hepnos-service.h"

void usage(void)
{
    fprintf(stderr, "Usage: hepnos-daemon <config-file> <connection-file>\n");
    fprintf(stderr, "  <config-file> path to the YAML file containing the service configuration\n");
    fprintf(stderr, "  <connection-file>  path to the YAML file to generate for clients\n");
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

