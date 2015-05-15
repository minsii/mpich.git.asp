/*
 * win_allocate_shared.c
 *  <FILE_DESC>
 * 	
 *  Author: Min Si
 */
#include <stdio.h>
#include <stdlib.h>
#include "mtcore.h"

int MPI_Win_allocate_shared(MPI_Aint size, int disp_unit, MPI_Info info, MPI_Comm comm,
                            void *baseptr, MPI_Win * win)
{
    int mpi_errno = MPI_SUCCESS;

    MTCORE_DBG_PRINT_FCNAME();
    MTCORE_RM_COUNT(MTCORE_RM_COMM_FREQ);

    if (comm == MPI_COMM_WORLD)
        comm = MTCORE_COMM_USER_WORLD;
    mpi_errno = PMPI_Win_allocate_shared(size, disp_unit, info, comm, baseptr, win);

    MTCORE_WARN_PRINT("called PMPI_Win_allocate_shared, no asynchronous progress on win 0x%x\n",
                      *win);

    return mpi_errno;
}
