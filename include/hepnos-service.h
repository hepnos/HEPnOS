#ifndef __HEPNOS_SERVICE_H
#define __HEPNOS_SERVICE_H

#include <mpi.h>

#ifdef __cplusplus
extern "C" {
#endif

void hepnos_run_service(MPI_Comm comm, const char* config_file, const char* connection_file);

#ifdef __cplusplus
}
#endif

#endif
