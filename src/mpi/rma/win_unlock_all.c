#include <stdio.h>
#include <stdlib.h>
#include "mtcore.h"

static inline int MTCORE_Win_unlock_self_impl(MTCORE_Win * uh_win)
{
    int mpi_errno = MPI_SUCCESS;

#ifdef MTCORE_ENABLE_SYNC_ALL_OPT
    /* unlockall already released window for local target */
#else
    int user_rank;
    PMPI_Comm_rank(uh_win->user_comm, &user_rank);

    if (uh_win->is_self_locked) {
        /* We need also release the lock of local rank */

        MTCORE_DBG_PRINT("[%d]unlock self(%d, local win 0x%x)\n", user_rank,
                         uh_win->my_rank_in_uh_comm, uh_win->my_uh_win);
        mpi_errno = PMPI_Win_unlock(uh_win->my_rank_in_uh_comm, uh_win->my_uh_win);
        if (mpi_errno != MPI_SUCCESS)
            return mpi_errno;
    }
#endif

    uh_win->is_self_locked = 0;
    return mpi_errno;
}

static int MTCORE_Win_mixed_unlock_all_impl(MPI_Win win, MTCORE_Win * uh_win)
{
    int mpi_errno = MPI_SUCCESS;
    int user_rank, user_nprocs;
    int i, j, k;

    PMPI_Comm_rank(uh_win->user_comm, &user_rank);
    PMPI_Comm_size(uh_win->user_comm, &user_nprocs);

#ifdef MTCORE_ENABLE_SYNC_ALL_OPT

    /* Optimization for MPI implementations that have optimized lock_all.
     * However, user should be noted that, if MPI implementation issues lock messages
     * for every target even if it does not have any operation, this optimization
     * could lose performance and even lose asynchronous! */
    for (i = 0; i < uh_win->num_uh_wins; i++) {
        MTCORE_DBG_PRINT("[%d]unlock_all(uh_win 0x%x)\n", user_rank, uh_win->uh_wins[i]);
        mpi_errno = PMPI_Win_unlock_all(uh_win->uh_wins[i]);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;
    }
#else
    for (i = 0; i < user_nprocs; i++) {
        for (k = 0; k < MTCORE_ENV.num_h; k++) {
            int target_h_rank_in_uh = uh_win->targets[i].h_ranks_in_uh[k];

            MTCORE_DBG_PRINT("[%d]unlock(Helper(%d), uh_win 0x%x), instead of "
                             "target rank %d\n", user_rank, target_h_rank_in_uh,
                             uh_win->targets[i].uh_win, i);
            mpi_errno = PMPI_Win_unlock(target_h_rank_in_uh, uh_win->targets[i].uh_win);
            if (mpi_errno != MPI_SUCCESS)
                goto fn_fail;
        }
    }
#endif

#ifdef MTCORE_ENABLE_LOCAL_LOCK_OPT
    mpi_errno = MTCORE_Win_unlock_self_impl(uh_win);
    if (mpi_errno != MPI_SUCCESS)
        goto fn_fail;
#endif

  fn_exit:
    return mpi_errno;

  fn_fail:
    goto fn_exit;
}

int MPI_Win_unlock_all(MPI_Win win)
{
    MTCORE_Win *uh_win;
    int mpi_errno = MPI_SUCCESS;
    int user_rank, user_nprocs;
    int i, j, k;

    MTCORE_DBG_PRINT_FCNAME();
    MTCORE_RM_COUNT(MTCORE_RM_COMM_FREQ);

    MTCORE_Fetch_uh_win_from_cache(win, uh_win);

    if (uh_win == NULL) {
        /* normal window */
        return PMPI_Win_unlock_all(win);
    }

    /* mtcore window starts */

    MTCORE_Assert((uh_win->info_args.epoch_type & MTCORE_EPOCH_LOCK) ||
                  (uh_win->info_args.epoch_type & MTCORE_EPOCH_LOCK_ALL));

    PMPI_Comm_rank(uh_win->user_comm, &user_rank);
    PMPI_Comm_size(uh_win->user_comm, &user_nprocs);

    for (i = 0; i < user_nprocs; i++) {
        uh_win->targets[i].remote_lock_assert = 0;
    }

    if (!(uh_win->info_args.epoch_type & MTCORE_EPOCH_LOCK)) {

        /* In lock_all only epoch, unlock all helpers on the single window. */

#ifdef MTCORE_ENABLE_SYNC_ALL_OPT

        /* Optimization for MPI implementations that have optimized lock_all.
         * However, user should be noted that, if MPI implementation issues lock messages
         * for every target even if it does not have any operation, this optimization
         * could lose performance and even lose asynchronous! */
        MTCORE_DBG_PRINT("[%d]unlock_all(uh_win 0x%x)\n", user_rank, uh_win->uh_wins[0]);
        mpi_errno = PMPI_Win_unlock_all(uh_win->uh_wins[0]);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;
#else
        mpi_errno = PMPI_Win_unlock_all(uh_win->uh_wins[0]);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;
#if 0   /* segmentation fault */
        for (i = 0; i < uh_win->num_h_ranks_in_uh; i++) {
            mpi_errno = PMPI_Win_unlock(uh_win->h_ranks_in_uh[i], uh_win->uh_wins[0]);
            if (mpi_errno != MPI_SUCCESS)
                goto fn_fail;
        }
#endif
#endif

#ifdef MTCORE_ENABLE_LOCAL_LOCK_OPT
#if 0   /* segmentation fault */
        mpi_errno = MTCORE_Win_unlock_self_impl(uh_win);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;
#else
        uh_win->is_self_locked = 0;
#endif
#endif
    }
    else {

        /* In lock_all/lock mixed epoch, separate windows are bound with each target. */
        mpi_errno = MTCORE_Win_mixed_unlock_all_impl(win, uh_win);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;
    }

#if defined(MTCORE_ENABLE_RUNTIME_LOAD_OPT)
    for (i = 0; i < user_nprocs; i++) {
        for (j = 0; j < uh_win->targets[i].num_segs; j++) {
            uh_win->targets[i].segs[j].main_lock_stat = MTCORE_MAIN_LOCK_RESET;
        }
    }
#endif

    /* Decrease lock/lockall counter, change epoch status only when counter
     * become 0. */
    uh_win->lockall_counter--;
    if (uh_win->lockall_counter == 0 && uh_win->lock_counter == 0) {
        MTCORE_DBG_PRINT("all locks are cleared ! no epoch now\n");
        uh_win->epoch_stat = MTCORE_WIN_NO_EPOCH;
    }

    /* TODO: All the operations which we have not wrapped up will be failed, because they
     * are issued to user window. We need wrap up all operations.
     */

  fn_exit:
    return mpi_errno;

  fn_fail:
    goto fn_exit;
}
