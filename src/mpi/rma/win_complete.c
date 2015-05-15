/*
 * win_complete.c
 *  <FILE_DESC>
 * 	
 *  Author: Min Si
 */

#include <stdio.h>
#include <stdlib.h>
#include "mtcore.h"

static int MTCORE_Send_pscw_complete_msg(int start_grp_size, MTCORE_Win * uh_win)
{
    int mpi_errno = MPI_SUCCESS;
    int i, user_rank;
    char comp_flg = 1;
    MPI_Request *reqs = NULL;
    MPI_Status *stats = NULL;
    int remote_cnt = 0;

    reqs = calloc(start_grp_size, sizeof(MPI_Request));
    stats = calloc(start_grp_size, sizeof(MPI_Status));

    PMPI_Comm_rank(uh_win->user_comm, &user_rank);

    for (i = 0; i < start_grp_size; i++) {
        int target_rank = uh_win->start_ranks_in_win_group[i];

        /* Do not send to local target, otherwise it may deadlock.
         * We do not check the wrong sync case that user calls wait(self)
         * before complete(self). */
        if (user_rank == target_rank)
            continue;

        mpi_errno = PMPI_Isend(&comp_flg, 1, MPI_CHAR, target_rank,
                               MTCORE_PSCW_CW_TAG, uh_win->user_comm, &reqs[remote_cnt++]);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;

        /* Set post flag to true on the main helper of post origin. */
        MTCORE_DBG_PRINT("send pscw complete msg to target %d \n", target_rank);
    }

    /* Has to blocking wait here to poll progress. */
    mpi_errno = PMPI_Waitall(remote_cnt, reqs, stats);
    if (mpi_errno != MPI_SUCCESS)
        goto fn_fail;

  fn_exit:
    if (reqs)
        free(reqs);
    if (stats)
        free(stats);
    return mpi_errno;

  fn_fail:
    goto fn_exit;
}

static int MTCORE_Complete_flush(int start_grp_size, MTCORE_Win * uh_win)
{
    int mpi_errno = MPI_SUCCESS;
    int user_rank, user_nprocs;
    int i, j, k;

    MTCORE_DBG_PRINT_FCNAME();

    PMPI_Comm_rank(uh_win->user_comm, &user_rank);

    /* Flush helpers to finish the sequence of locally issued RMA operations */
#ifdef MTCORE_ENABLE_SYNC_ALL_OPT

    /* Optimization for MPI implementations that have optimized lock_all.
     * However, user should be noted that, if MPI implementation issues lock messages
     * for every target even if it does not have any operation, this optimization
     * could lose performance and even lose asynchronous! */
    MTCORE_DBG_PRINT("[%d]flush_all(active_win 0x%x)\n", user_rank, uh_win->active_win);
    mpi_errno = PMPI_Win_flush_all(uh_win->active_win);
    if (mpi_errno != MPI_SUCCESS)
        goto fn_fail;
#else

    /* Flush every helper once in the single window.
     * TODO: track op issuing, only flush the helpers which receive ops. */
    for (i = 0; i < uh_win->num_h_ranks_in_uh; i++) {
        mpi_errno = PMPI_Win_flush(uh_win->h_ranks_in_uh[i], uh_win->active_win);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;
    }

    if (uh_win->info_args.async_config == MTCORE_ASYNC_CONFIG_AUTO) {
        /* flush targets which are in async-off state. Only happen with automatic
         * asynchronous configuration. */
        for (i = 0; i < start_grp_size; i++) {
            if (uh_win->targets[i].async_stat == MTCORE_ASYNC_STAT_OFF) {
                mpi_errno = PMPI_Win_flush(uh_win->targets[i].uh_rank, uh_win->active_win);
                if (mpi_errno != MPI_SUCCESS)
                    goto fn_fail;
            }
        }
    }

#ifdef MTCORE_ENABLE_LOCAL_LOCK_OPT
    /* Need flush local target */
    for (i = 0; i < start_grp_size; i++) {
        if (uh_win->start_ranks_in_win_group[i] == user_rank) {
            mpi_errno = PMPI_Win_flush(uh_win->my_rank_in_uh_comm, uh_win->active_win);
            if (mpi_errno != MPI_SUCCESS)
                goto fn_fail;
        }
    }
#endif

#endif

    /* TODO: All the operations which we have not wrapped up will be failed, because they
     * are issued to user window. We need wrap up all operations.
     */

  fn_exit:
    return mpi_errno;

  fn_fail:
    goto fn_exit;
}

int MPI_Win_complete(MPI_Win win)
{
    MTCORE_Win *uh_win;
    int mpi_errno = MPI_SUCCESS;
    int start_grp_size = 0;
    int i;

    MTCORE_DBG_PRINT_FCNAME();
    MTCORE_RM_TIMER_STR(MTCORE_RM_COMM_TIME);

    MTCORE_Fetch_uh_win_from_cache(win, uh_win);

    if (uh_win == NULL) {
        /* normal window */
        return PMPI_Win_complete(win);
    }

    MTCORE_Assert((uh_win->info_args.epoch_type & MTCORE_EPOCH_PSCW));

    if (uh_win->start_group == MPI_GROUP_NULL) {
        /* standard says do nothing for empty group */
        MTCORE_DBG_PRINT("Complete empty group\n");
        return mpi_errno;
    }

    mpi_errno = PMPI_Group_size(uh_win->start_group, &start_grp_size);
    if (mpi_errno != MPI_SUCCESS)
        goto fn_fail;
    MTCORE_Assert(start_grp_size > 0);

    MTCORE_DBG_PRINT("Complete group 0x%x, size %d\n", uh_win->start_group, start_grp_size);

    mpi_errno = MTCORE_Complete_flush(start_grp_size, uh_win);
    if (mpi_errno != MPI_SUCCESS)
        goto fn_fail;

    uh_win->is_self_locked = 0;

    mpi_errno = MTCORE_Send_pscw_complete_msg(start_grp_size, uh_win);
    if (mpi_errno != MPI_SUCCESS)
        goto fn_fail;

    /* Indicate epoch status, later operations should not be redirected to active_win
     * after the start counter decreases to 0 .*/
    uh_win->start_counter--;
    if (uh_win->start_counter == 0) {
        uh_win->epoch_stat = MTCORE_WIN_NO_EPOCH;
    }

    MTCORE_DBG_PRINT("Complete done\n");

  fn_exit:
    MTCORE_RM_TIMER_END(MTCORE_RM_COMM_TIME);
    if (uh_win->start_ranks_in_win_group)
        free(uh_win->start_ranks_in_win_group);
    uh_win->start_group = MPI_GROUP_NULL;
    uh_win->start_ranks_in_win_group = NULL;

    return mpi_errno;

  fn_fail:
    goto fn_exit;

}
