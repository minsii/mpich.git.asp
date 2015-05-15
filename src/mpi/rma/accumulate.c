#include <stdio.h>
#include <stdlib.h>
#include "mtcore.h"

static int MTCORE_Accumulate_segment_impl(const void *origin_addr,
                                          int origin_count,
                                          MPI_Datatype origin_datatype, int target_rank,
                                          MPI_Aint target_disp,
                                          int target_count, MPI_Datatype target_datatype,
                                          MPI_Op op, MPI_Win win, MTCORE_Win * uh_win)
{
    int mpi_errno = MPI_SUCCESS;
    int num_segs = 0, i;
    MTCORE_OP_Segment *decoded_ops = NULL;

    /* TODO : Eliminate operation division for some special cases, see pptx */
    mpi_errno = MTCORE_Op_segments_decode(origin_addr, origin_count,
                                          origin_datatype, target_rank, target_disp, target_count,
                                          target_datatype, uh_win, &decoded_ops, &num_segs);
    if (mpi_errno != MPI_SUCCESS)
        goto fn_fail;

    MTCORE_DBG_PRINT("MTCORE Accumulate to target %d, num_segs=%d\n", target_rank, num_segs);

    for (i = 0; i < num_segs; i++) {
        int target_h_rank_in_uh = -1;
        int data_size = 0;
        MPI_Aint target_h_offset = 0;
        MPI_Aint uh_target_disp = 0;
        int seg_off = decoded_ops[i].target_seg_off;
        MPI_Win seg_uh_win = uh_win->targets[target_rank].segs[seg_off].uh_win;

        mpi_errno = MTCORE_Get_helper_rank(target_rank, seg_off, 1, decoded_ops[i].target_dtsize,
                                           uh_win, &target_h_rank_in_uh, &target_h_offset);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;

        uh_target_disp = target_h_offset
            + uh_win->targets[target_rank].disp_unit * decoded_ops[i].target_disp;

        /* Issue operation to the helper process in corresponding uh-window of target process. */
        mpi_errno = PMPI_Accumulate(decoded_ops[i].origin_addr, decoded_ops[i].origin_count,
                                    decoded_ops[i].origin_datatype, target_h_rank_in_uh,
                                    uh_target_disp, decoded_ops[i].target_count,
                                    decoded_ops[i].target_datatype, op, seg_uh_win);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;

        MTCORE_DBG_PRINT("MTCORE Accumulate to (helper %d, win 0x%x) instead of "
                         "target %d, seg %d \n"
                         "(origin.addr %p, count %d, datatype 0x%x, "
                         "target.disp 0x%lx(0x%lx + %d * %ld), count %d, datatype 0x%x)\n",
                         target_h_rank_in_uh, seg_uh_win, target_rank, seg_off,
                         decoded_ops[i].origin_addr, decoded_ops[i].origin_count,
                         decoded_ops[i].origin_datatype, uh_target_disp, target_h_offset,
                         uh_win->targets[target_rank].disp_unit, decoded_ops[i].target_disp,
                         decoded_ops[i].target_count, decoded_ops[i].target_datatype);
    }

  fn_exit:
    MTCORE_Op_segments_destroy(&decoded_ops);
    return mpi_errno;

  fn_fail:
    goto fn_exit;
}

static int MTCORE_Accumulate_impl(const void *origin_addr, int origin_count,
                                  MPI_Datatype origin_datatype,
                                  int target_rank, MPI_Aint target_disp,
                                  int target_count,
                                  MPI_Datatype target_datatype, MPI_Op op, MPI_Win win,
                                  MTCORE_Win * uh_win)
{
    int mpi_errno = MPI_SUCCESS;
    MPI_Aint uh_target_disp = 0;
    int is_shared = 0;

    int rank;

    PMPI_Comm_rank(uh_win->user_comm, &rank);

    /* Should not do local RMA in accumulate because of atomicity issue */

    /* TODO: Do we need segment load balancing in fence ?
     * 1. No lock issue.
     * 2. overhead of data range checking and division */
    if (MTCORE_ENV.lock_binding == MTCORE_LOCK_BINDING_SEGMENT &&
        uh_win->targets[target_rank].num_segs > 1 && uh_win->epoch_stat == MTCORE_WIN_EPOCH_LOCK) {
        mpi_errno = MTCORE_Accumulate_segment_impl(origin_addr, origin_count,
                                                   origin_datatype, target_rank, target_disp,
                                                   target_count, target_datatype, op, win, uh_win);
        if (mpi_errno != MPI_SUCCESS)
            return mpi_errno;
    }
    else {
        /* Translation for intra/inter-node operations.
         *
         * We do not use force flush + shared window for optimizing operations to local targets.
         * Because: 1) we lose lock optimization on force flush; 2) Although most implementation
         * does shared-communication for operations on shared windows, MPI standard doesn’t
         * require it. Some implementation may use network even for shared targets for
         * shorter CPU occupancy.
         */
        int target_h_rank_in_uh = -1;
        int data_size = 0;
        MPI_Aint target_h_offset = 0;
        MPI_Win *win_ptr = NULL;

        MTCORE_Get_epoch_win(target_rank, 0, uh_win, win_ptr);

#if defined(MTCORE_ENABLE_RUNTIME_LOAD_OPT)
        if (MTCORE_ENV.load_opt == MTCORE_LOAD_BYTE_COUNTING) {
            PMPI_Type_size(origin_datatype, &data_size);
            data_size *= origin_count;
        }
#endif
        mpi_errno = MTCORE_Get_helper_rank(target_rank, 0, 1, data_size, uh_win,
                                           &target_h_rank_in_uh, &target_h_offset);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;

        uh_target_disp = target_h_offset + uh_win->targets[target_rank].disp_unit * target_disp;

        /* Issue operation to the helper process in corresponding uh-window of target process. */
        mpi_errno = PMPI_Accumulate(origin_addr, origin_count, origin_datatype,
                                    target_h_rank_in_uh, uh_target_disp,
                                    target_count, target_datatype, op, *win_ptr);

        MTCORE_DBG_PRINT("MTCORE Accumulate to (helper %d, win 0x%x [%s]) instead of "
                         "target %d, 0x%lx(0x%lx + %d * %ld)\n",
                         target_h_rank_in_uh, *win_ptr,
                         MTCORE_Win_epoch_stat_name[uh_win->epoch_stat],
                         target_rank, uh_target_disp, target_h_offset,
                         uh_win->targets[target_rank].disp_unit, target_disp);
    }

  fn_exit:

    return mpi_errno;

  fn_fail:

    goto fn_exit;
}

int MPI_Accumulate(const void *origin_addr, int origin_count,
                   MPI_Datatype origin_datatype,
                   int target_rank, MPI_Aint target_disp,
                   int target_count, MPI_Datatype target_datatype, MPI_Op op, MPI_Win win)
{
    static const char FCNAME[] = "MPI_Accumulate";
    int mpi_errno = MPI_SUCCESS;
    MTCORE_Win *uh_win;

    MTCORE_DBG_PRINT_FCNAME();
    MTCORE_RM_TIMER_STR(MTCORE_RM_COMM_TIME);

    MTCORE_Fetch_uh_win_from_cache(win, uh_win);

    if (uh_win == NULL) {
        mpi_errno = PMPI_Accumulate(origin_addr, origin_count,
                                    origin_datatype, target_rank, target_disp, target_count,
                                    target_datatype, op, win);
    }
    /* If the target is in async-off state, directly send to the target via internal window. */
    else if (uh_win->targets[target_rank].async_stat == MTCORE_ASYNC_STAT_OFF) {
        MPI_Win *win_ptr = NULL;
        MTCORE_Get_epoch_win(target_rank, 0, uh_win, win_ptr);

        mpi_errno = PMPI_Accumulate(origin_addr, origin_count, origin_datatype,
                                    uh_win->targets[target_rank].uh_rank, target_disp,
                                    target_count, target_datatype, op, *win_ptr);
        MTCORE_DBG_PRINT("Accumulate to (target %d, win 0x%x [%s]) "
                         "instead of target %d\n",
                         uh_win->targets[target_rank].uh_rank, *win_ptr,
                         MTCORE_Win_epoch_stat_name[uh_win->epoch_stat], target_rank);
    }
    else {
        /* mtcore window */
        mpi_errno = MTCORE_Accumulate_impl(origin_addr, origin_count,
                                           origin_datatype, target_rank, target_disp, target_count,
                                           target_datatype, op, win, uh_win);
    }

  fn_exit:
    MTCORE_RM_TIMER_END(MTCORE_RM_COMM_TIME);
    return mpi_errno;

  fn_fail:

    goto fn_exit;
}
