#ifndef __HEPNOS_SERVICE_H
#define __HEPNOS_SERVICE_H

#include <mpi.h>

#ifdef __cplusplus
extern "C" {
#endif

void hepnos_run_service(MPI_Comm comm, const char* listen_addr, const char* config_file);

#ifdef __cplusplus
}
#endif

#endif
