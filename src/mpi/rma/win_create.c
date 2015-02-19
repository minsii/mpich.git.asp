/*
 * win_create.c
 *  <FILE_DESC>
 * 	
 *  Author: Min Si
 */
#include <stdio.h>
#include <stdlib.h>
#include "mtcore.h"

int MPI_Win_create(void *base, MPI_Aint size, int disp_unit, MPI_Info info,
                   MPI_Comm comm, MPI_Win * win)
{
    int mpi_errno = MPI_SUCCESS;

    MTCORE_DBG_PRINT_FCNAME();
    MTCORE_RM_TIMER_STR(MTCORE_RM_COMM_TIME);

    if (comm == MPI_COMM_WORLD)
        comm = MTCORE_COMM_USER_WORLD;

    mpi_errno = PMPI_Win_create(base, size, disp_unit, info, comm, win);

    MTCORE_WARN_PRINT("called MPI_Win_create, no asynchronous progress on win 0x%x\n", *win);

    MTCORE_RM_TIMER_END(MTCORE_RM_COMM_TIME);
    return mpi_errno;
}
