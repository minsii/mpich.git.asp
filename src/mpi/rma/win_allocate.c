#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <memory.h>
#include "mtcore.h"

#include <ctype.h>

const char *MTCORE_Win_epoch_stat_name[4] = {
    "NO_EPOCH",
    "FENCE",
    "LOCK",
    "PSCW"
};


static int read_win_info(MPI_Info info, MTCORE_Win * uh_win)
{
    int mpi_errno = MPI_SUCCESS;

    uh_win->info_args.no_local_load_store = 0;
    uh_win->info_args.epoch_type = MTCORE_EPOCH_LOCK_ALL | MTCORE_EPOCH_LOCK |
        MTCORE_EPOCH_PSCW | MTCORE_EPOCH_FENCE;
    uh_win->info_args.enable_async = 1;

    if (info != MPI_INFO_NULL) {
        int info_flag = 0;
        char info_value[MPI_MAX_INFO_VAL + 1];

        /* Check if user wants to turn off async */
        memset(info_value, 0, sizeof(info_value));
        mpi_errno = PMPI_Info_get(info, "enable_async", MPI_MAX_INFO_VAL, info_value, &info_flag);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;

        if (info_flag == 1) {
            if (!strncmp(info_value, "false", strlen("false"))) {
                uh_win->info_args.enable_async = 0;     /* off */
            }
            else if (!strncmp(info_value, "true", strlen("true"))) {
                uh_win->info_args.enable_async = 2;     /* force on */
            }

            MTCORE_DBG_PRINT("user sets enable_async=%d\n", uh_win->info_args.enable_async);
        }

        /* If async is off, no need to do any future work */
        if (uh_win->info_args.enable_async == 0) {
            goto fn_exit;
        }

        /* Check if we are allowed to ignore force-lock for local target,
         * require force-lock by default. */
        memset(info_value, 0, sizeof(info_value));
        mpi_errno = PMPI_Info_get(info, "no_local_load_store", MPI_MAX_INFO_VAL,
                                  info_value, &info_flag);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;

        if (info_flag == 1) {
            if (!strncmp(info_value, "true", strlen("true")))
                uh_win->info_args.no_local_load_store = 1;
        }

        /* Check if user specifies epoch types */
        memset(info_value, 0, sizeof(info_value));
        mpi_errno = PMPI_Info_get(info, "epoch_type", MPI_MAX_INFO_VAL, info_value, &info_flag);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;

        if (info_flag == 1) {
            int user_epoch_type = 0;
            char *type = NULL;

            type = strtok(info_value, "|");
            while (type != NULL) {
                if (!strncmp(type, "lockall", strlen("lockall"))) {
                    user_epoch_type |= MTCORE_EPOCH_LOCK_ALL;
                }
                else if (!strncmp(type, "lock", strlen("lock"))) {
                    user_epoch_type |= MTCORE_EPOCH_LOCK;
                }
                else if (!strncmp(type, "pscw", strlen("pscw"))) {
                    user_epoch_type |= MTCORE_EPOCH_PSCW;
                }
                else if (!strncmp(type, "fence", strlen("fence"))) {
                    user_epoch_type |= MTCORE_EPOCH_FENCE;
                }
                type = strtok(NULL, "|");
            }

            if (user_epoch_type != 0)
                uh_win->info_args.epoch_type = user_epoch_type;
        }
    }

    MTCORE_DBG_PRINT("no_local_load_store %d, epoch_type=%s|%s|%s|%s\n",
                     uh_win->info_args.no_local_load_store,
                     ((uh_win->info_args.epoch_type & MTCORE_EPOCH_LOCK_ALL) ? "lockall" : ""),
                     ((uh_win->info_args.epoch_type & MTCORE_EPOCH_LOCK) ? "lock" : ""),
                     ((uh_win->info_args.epoch_type & MTCORE_EPOCH_PSCW) ? "pscw" : ""),
                     ((uh_win->info_args.epoch_type & MTCORE_EPOCH_FENCE) ? "fence" : "")
);

  fn_exit:
    return mpi_errno;

  fn_fail:
    goto fn_exit;
}

static int gather_ranks(MTCORE_Win * win, int *num_helpers, int *helper_ranks_in_world,
                        int *unique_helper_ranks_in_world)
{
    int mpi_errno = MPI_SUCCESS;
    int user_nprocs;
    int *helper_bitmap = NULL;
    int user_world_rank, tmp_num_helpers;
    int i, j, helper_rank;
    int world_nprocs;

    PMPI_Comm_size(MPI_COMM_WORLD, &world_nprocs);
    PMPI_Comm_size(win->user_comm, &user_nprocs);

    helper_bitmap = calloc(world_nprocs, sizeof(int));
    if (helper_bitmap == NULL)
        goto fn_fail;

    /* Get helper ranks of each USER process.
     *
     * The helpers of user_world rank x are stored as x*num_h: (x+1)*num_h-1,
     * it is used to catch helpers for a target rank in epoch.
     * Unique helper ranks are only used for creating communicators.*/
    tmp_num_helpers = 0;
    for (i = 0; i < user_nprocs; i++) {
        user_world_rank = win->targets[i].user_world_rank;

        for (j = 0; j < MTCORE_ENV.num_h; j++) {
            helper_rank = MTCORE_ALL_H_RANKS_IN_WORLD[user_world_rank * MTCORE_ENV.num_h + j];
            helper_ranks_in_world[i * MTCORE_ENV.num_h + j] = helper_rank;

            /* Unique helper ranks */
            if (!helper_bitmap[helper_rank]) {
                unique_helper_ranks_in_world[tmp_num_helpers++] = helper_rank;
                helper_bitmap[helper_rank] = 1;

                MTCORE_Assert(tmp_num_helpers <= MTCORE_NUM_NODES * MTCORE_ENV.num_h);
            }
        }
    }
    *num_helpers = tmp_num_helpers;

  fn_exit:
    if (helper_bitmap)
        free(helper_bitmap);
    return mpi_errno;

  fn_fail:
    goto fn_exit;
}

static void specify_main_helper_binding_by_segments(int n_targets, int *local_targets,
                                                    MTCORE_Win * uh_win)
{
    MPI_Aint seg_size, t_size, sum_size, size_per_helper, assigned_size, max_t_size;
    int t_num_segs, t_last_h_off, max_t_num_seg;
    int i, j, h_off, t_rank, user_nprocs;
    MPI_Aint *t_seg_sizes = NULL;

    PMPI_Comm_size(uh_win->user_comm, &user_nprocs);

    /* Calculate size binded to each helper */
    max_t_size = 0;
    sum_size = 0;
    for (i = 0; i < n_targets; i++) {
        t_rank = local_targets[i];
        MTCORE_Assert(t_rank < user_nprocs);

        sum_size += uh_win->targets[t_rank].size;
        max_t_size = max(max_t_size, uh_win->targets[t_rank].size);
    }
    /* Never divide less than segment unit */
    size_per_helper = align(sum_size / MTCORE_ENV.num_h, MTCORE_SEGMENT_UNIT);
    max_t_num_seg = sum_size / size_per_helper + 3;
    t_seg_sizes = calloc(max_t_num_seg, sizeof(MPI_Aint));

    /* Divide segments based on sizes */
    i = 0;
    t_rank = local_targets[0];
    h_off = 0;
    t_last_h_off = 0;
    t_size = uh_win->targets[0].size;
    t_num_segs = 0;
    assigned_size = 0;
    seg_size = size_per_helper;
    while (assigned_size < sum_size && i < n_targets) {
        if (t_size == 0) {
            uh_win->targets[t_rank].num_segs = t_num_segs;
            uh_win->targets[t_rank].segs = calloc(t_num_segs, sizeof(MTCORE_Win_target_seg));

            MPI_Aint prev_seg_base = 0, prev_seg_size = 0;
            for (j = 0; j < t_num_segs; j++) {
                uh_win->targets[t_rank].segs[j].base_offset = prev_seg_base + prev_seg_size;
                uh_win->targets[t_rank].segs[j].size = t_seg_sizes[j];
                uh_win->targets[t_rank].segs[j].main_h_off = t_last_h_off + 1 - t_num_segs + j;

                MTCORE_Assert(uh_win->targets[t_rank].segs[j].main_h_off < MTCORE_ENV.num_h);

                prev_seg_base = uh_win->targets[t_rank].segs[j].base_offset;
                prev_seg_size = uh_win->targets[t_rank].segs[j].size;
            }

            assigned_size += uh_win->targets[t_rank].size;

            /* next target */
            if (i >= n_targets) {
                break;
            }
            t_rank = local_targets[++i];
            t_size = uh_win->targets[t_rank].size;
            t_num_segs = 0;
        }
        /* finish this target if remaining size is small than seg_size or
         * it is already at the last helper. */
        else if (t_size < seg_size || h_off == MTCORE_ENV.num_h - 1) {
            MTCORE_Assert(t_num_segs < max_t_num_seg);

            t_seg_sizes[t_num_segs++] = t_size;
            seg_size -= t_size;
            /* make sure remaining segment size is aligned */
            seg_size = align(seg_size, MTCORE_SEGMENT_UNIT);

            t_size = 0;

            t_last_h_off = h_off;
        }
        else if (t_size >= seg_size) {
            MTCORE_Assert(t_num_segs < max_t_num_seg);

            /* divide large target */
            t_seg_sizes[t_num_segs++] = seg_size;
            t_size -= seg_size;
            seg_size = 0;

            t_last_h_off = h_off;
        }

        /* next helper */
        if (seg_size == 0) {
            h_off++;
            MTCORE_Assert(h_off <= MTCORE_ENV.num_h);
            seg_size = size_per_helper
                + (h_off == MTCORE_ENV.num_h - 1 ? (sum_size % MTCORE_ENV.num_h) : 0);
            /* make sure new segment size is aligned */
            seg_size = align(seg_size, MTCORE_SEGMENT_UNIT);
        }
    }

    MTCORE_Assert(assigned_size == sum_size);

    if (t_seg_sizes)
        free(t_seg_sizes);
}

static void specify_main_helper_binding_by_ranks(int n_targets, int *local_targets,
                                                 MTCORE_Win * uh_win)
{
    int i, h_off, t_rank, user_nprocs;
    int np_per_helper;

    PMPI_Comm_size(uh_win->user_comm, &user_nprocs);

    np_per_helper = n_targets / MTCORE_ENV.num_h;

    int np = np_per_helper;
    i = 0;
    h_off = 0;
    t_rank = local_targets[i];

    while (i < n_targets) {
        if (np == 0) {
            /* next helper */
            h_off++;
            np = np_per_helper +
                ((h_off == MTCORE_ENV.num_h - 1) ? (n_targets % MTCORE_ENV.num_h) : 0);
        }
        MTCORE_Assert(h_off <= MTCORE_ENV.num_h);

        t_rank = local_targets[i];
        uh_win->targets[t_rank].num_segs = 1;
        uh_win->targets[t_rank].segs = calloc(1, sizeof(MTCORE_Win_target_seg));
        uh_win->targets[t_rank].segs[0].base_offset = 0;
        uh_win->targets[t_rank].segs[0].size = uh_win->targets[i].size;
        uh_win->targets[t_rank].segs[0].main_h_off = h_off;

        /* next target */
        i++;
        np--;
    }
}

static void specify_main_helper_binding(MTCORE_Win * uh_win)
{
    int i, j, h_off, user_nprocs;
    int *local_targets = NULL;

    PMPI_Comm_size(uh_win->user_comm, &user_nprocs);
    local_targets = calloc(uh_win->num_nodes * uh_win->max_local_user_nprocs, sizeof(int));

    /* Sort targets by node_ids */
    for (i = 0; i < user_nprocs; i++) {
        int off = uh_win->targets[i].node_id * uh_win->max_local_user_nprocs +
            uh_win->targets[i].local_user_rank;
        local_targets[off] = i;
    }

    /* Specify main helpers on each node */
    if (MTCORE_ENV.lock_binding == MTCORE_LOCK_BINDING_SEGMENT) {
        for (i = 0; i < uh_win->num_nodes; i++) {
            int s_off = i * uh_win->max_local_user_nprocs;
            int s_rank = local_targets[s_off];
            int n_targets = uh_win->targets[s_rank].local_user_nprocs;

            specify_main_helper_binding_by_segments(n_targets, &local_targets[s_off], uh_win);
        }
    }
    else {
        for (i = 0; i < uh_win->num_nodes; i++) {
            int s_off = i * uh_win->max_local_user_nprocs;
            int s_rank = local_targets[s_off];
            int n_targets = uh_win->targets[s_rank].local_user_nprocs;

            specify_main_helper_binding_by_ranks(n_targets, &local_targets[s_off], uh_win);
        }
    }

#ifdef DEBUG
    for (i = 0; i < user_nprocs; i++) {
        MTCORE_DBG_PRINT("\t target[%d] .num_segs %d\n", i, uh_win->targets[i].num_segs);
        for (j = 0; j < MTCORE_ENV.num_h; j++) {
            MTCORE_DBG_PRINT("\t\t .h_rank[%d] %d, offset[%d] 0x%lx \n",
                             j, uh_win->targets[i].h_ranks_in_uh[j],
                             j, uh_win->targets[i].base_h_offsets[j]);
        }
        for (j = 0; j < uh_win->targets[i].num_segs; j++) {
            MTCORE_DBG_PRINT("\t\t .seg[%d].main_h_off=%d, base_offset=0x%lx, size=0x%x\n",
                             j, uh_win->targets[i].segs[j].main_h_off,
                             uh_win->targets[i].segs[j].base_offset,
                             uh_win->targets[i].segs[j].size);
        }
    }
#endif

    if (local_targets)
        free(local_targets);
}

static int create_uh_comm(int num_helpers, int *helper_ranks_in_world, MTCORE_Win * win)
{
    int mpi_errno = MPI_SUCCESS;
    int user_nprocs, user_rank, world_nprocs, tmp_world_rank;
    int *uh_ranks_in_world = NULL, *h_info = NULL;
    int i, j, num_uh_ranks, h_rank;

    PMPI_Comm_size(win->user_comm, &user_nprocs);
    PMPI_Comm_rank(win->user_comm, &user_rank);
    PMPI_Comm_size(MPI_COMM_WORLD, &world_nprocs);

    /* maximum amount equals to world size */
    uh_ranks_in_world = calloc(world_nprocs, sizeof(int));
    if (uh_ranks_in_world == NULL)
        goto fn_fail;

    /* -Create uh communicator including all USER processes and Helper processes. */
    num_uh_ranks = num_helpers;
    memcpy(uh_ranks_in_world, helper_ranks_in_world, num_helpers * sizeof(int));
    for (i = 0; i < user_nprocs; i++) {
        uh_ranks_in_world[num_uh_ranks++] = win->targets[i].world_rank;
    }
    if (num_uh_ranks > world_nprocs) {
        fprintf(stderr, "num_uh_ranks %d > world_nprocs %d, num_helpers=%d, user_nprocs=%d\n",
                num_uh_ranks, world_nprocs, num_helpers, user_nprocs);
    }
    MTCORE_Assert(num_uh_ranks <= world_nprocs);

    PMPI_Group_incl(MTCORE_GROUP_WORLD, num_uh_ranks, uh_ranks_in_world, &win->uh_group);
    mpi_errno = PMPI_Comm_create_group(MPI_COMM_WORLD, win->uh_group, 0, &win->uh_comm);
    if (mpi_errno != MPI_SUCCESS)
        goto fn_fail;

  fn_exit:
    if (uh_ranks_in_world)
        free(uh_ranks_in_world);

    return mpi_errno;

  fn_fail:
    if (win->uh_comm != MPI_COMM_NULL) {
        PMPI_Comm_free(&win->uh_comm);
        win->uh_comm = MPI_COMM_NULL;
    }
    if (win->uh_group != MPI_GROUP_NULL) {
        PMPI_Group_free(&win->uh_group);
        win->uh_group = MPI_GROUP_NULL;
    }

    goto fn_exit;
}

static int create_communicators(MTCORE_Win * uh_win)
{
    int mpi_errno = MPI_SUCCESS;
    int *func_params = NULL, func_param_size = 0;
    int *user_ranks_in_world = NULL;
    int *helper_ranks_in_world = NULL;
    int *user_ranks_in_uh = NULL;
    int num_helpers = 0, max_num_helpers;
    int user_nprocs, user_local_rank;
    int *helper_ranks_in_uh = NULL;
    int i;

    PMPI_Comm_size(uh_win->user_comm, &user_nprocs);
    max_num_helpers = MTCORE_ENV.num_h * MTCORE_NUM_NODES;
    func_param_size = user_nprocs + max_num_helpers + 2;

    PMPI_Comm_rank(uh_win->local_user_comm, &user_local_rank);
    uh_win->num_h_ranks_in_uh = MTCORE_ENV.num_h * uh_win->num_nodes;

    /* Optimization for user world communicator */
    if (uh_win->user_comm == MTCORE_COMM_USER_WORLD) {
        if (user_local_rank == 0) {
            func_params = calloc(func_param_size, sizeof(int));

            /* Set parameters to local Helpers
             *  [0]: is_comm_user_world
             */
            func_params[0] = 1;

            mpi_errno = MTCORE_Func_set_param((char *) func_params, sizeof(int) * func_param_size,
                                              uh_win->ur_h_comm);
            if (mpi_errno != MPI_SUCCESS)
                goto fn_fail;
        }

        /* Create communicators
         *  local_uh_comm: including local USER and Helper processes
         *  uh_comm: including all USER and Helper processes
         */
        uh_win->local_uh_comm = MTCORE_COMM_LOCAL;
        uh_win->uh_comm = MPI_COMM_WORLD;
        PMPI_Comm_group(uh_win->local_uh_comm, &uh_win->local_uh_group);
        PMPI_Comm_group(uh_win->uh_comm, &uh_win->uh_group);

        /* -Get all Helper rank in uh communicator */
        memcpy(uh_win->h_ranks_in_uh, MTCORE_ALL_UNIQUE_H_RANKS_IN_WORLD,
               uh_win->num_h_ranks_in_uh * sizeof(int));
        for (i = 0; i < user_nprocs; i++)
            memcpy(uh_win->targets[i].h_ranks_in_uh,
                   &MTCORE_ALL_H_RANKS_IN_WORLD[i * MTCORE_ENV.num_h],
                   sizeof(int) * MTCORE_ENV.num_h);

        /* -Get all user rank in uh communicator */
        for (i = 0; i < user_nprocs; i++)
            uh_win->targets[i].uh_rank = MTCORE_USER_RANKS_IN_WORLD[i];
    }
    else {
        /* helper ranks for every user process, used for helper fetching in epoch */
        helper_ranks_in_world = calloc(MTCORE_ENV.num_h * user_nprocs, sizeof(int));
        helper_ranks_in_uh = calloc(MTCORE_ENV.num_h * user_nprocs, sizeof(int));
        user_ranks_in_world = calloc(user_nprocs, sizeof(int));
        user_ranks_in_uh = calloc(user_nprocs, sizeof(int));

        for (i = 0; i < user_nprocs; i++) {
            user_ranks_in_world[i] = uh_win->targets[i].world_rank;
        }

        /* Gather user rank information */
        mpi_errno = gather_ranks(uh_win, &num_helpers, helper_ranks_in_world,
                                 uh_win->h_ranks_in_uh);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;

        if (user_local_rank == 0) {

            /* Set parameters to local Helpers
             *  [0]: is_comm_user_world
             *  [1]: num_helpers
             *  [2:N+1]: user ranks in comm_world
             *  [N+2:]: helper ranks in comm_world
             */
            int pidx;
            func_params = calloc(func_param_size, sizeof(int));

            func_params[0] = 0;
            func_params[1] = num_helpers;
            pidx = 2;
            memcpy(&func_params[pidx], user_ranks_in_world, user_nprocs * sizeof(int));
            pidx += user_nprocs;
            memcpy(&func_params[pidx], uh_win->h_ranks_in_uh, num_helpers * sizeof(int));
            mpi_errno = MTCORE_Func_set_param((char *) func_params, sizeof(int) * func_param_size,
                                              uh_win->ur_h_comm);
            if (mpi_errno != MPI_SUCCESS)
                goto fn_fail;
        }

        /* Create communicators
         *  uh_comm: including all USER and Helper processes
         *  local_uh_comm: including local USER and Helper processes
         */
        mpi_errno = create_uh_comm(num_helpers, uh_win->h_ranks_in_uh, uh_win);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;

#ifdef DEBUG
        {
            int uh_rank, uh_nprocs;
            PMPI_Comm_rank(uh_win->uh_comm, &uh_rank);
            PMPI_Comm_size(uh_win->uh_comm, &uh_nprocs);
            MTCORE_DBG_PRINT("created uh_comm, my rank %d/%d\n", uh_rank, uh_nprocs);
        }
#endif

        mpi_errno = PMPI_Comm_split_type(uh_win->uh_comm, MPI_COMM_TYPE_SHARED, 0,
                                         MPI_INFO_NULL, &uh_win->local_uh_comm);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;

#ifdef DEBUG
        {
            int uh_rank, uh_nprocs;
            PMPI_Comm_rank(uh_win->local_uh_comm, &uh_rank);
            PMPI_Comm_size(uh_win->local_uh_comm, &uh_nprocs);
            MTCORE_DBG_PRINT("created local_uh_comm, my rank %d/%d\n", uh_rank, uh_nprocs);
        }
#endif

        /* Get all Helper rank in uh communicator */
        mpi_errno = PMPI_Group_translate_ranks(MTCORE_GROUP_WORLD, user_nprocs * MTCORE_ENV.num_h,
                                               helper_ranks_in_world, uh_win->uh_group,
                                               helper_ranks_in_uh);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;

        PMPI_Comm_group(uh_win->local_uh_comm, &uh_win->local_uh_group);

        for (i = 0; i < user_nprocs; i++)
            memcpy(uh_win->targets[i].h_ranks_in_uh, &helper_ranks_in_uh[i * MTCORE_ENV.num_h],
                   sizeof(int) * MTCORE_ENV.num_h);

        /* -Get all user rank in uh communicator */
        mpi_errno = PMPI_Group_translate_ranks(MTCORE_GROUP_WORLD, user_nprocs,
                                               user_ranks_in_world, uh_win->uh_group,
                                               user_ranks_in_uh);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;
        for (i = 0; i < user_nprocs; i++)
            uh_win->targets[i].uh_rank = user_ranks_in_uh[i];
    }

#ifdef DEBUG
    {
        MTCORE_DBG_PRINT("%d unique h_ranks:\n", uh_win->num_h_ranks_in_uh);
        for (i = 0; i < uh_win->num_h_ranks_in_uh; i++) {
            MTCORE_DBG_PRINT("\t[%d] %d\n", i, uh_win->h_ranks_in_uh[i]);
        }
    }
#endif

  fn_exit:
    if (func_params)
        free(func_params);
    if (user_ranks_in_world)
        free(user_ranks_in_world);
    if (user_ranks_in_uh)
        free(user_ranks_in_uh);
    if (helper_ranks_in_world)
        free(helper_ranks_in_world);
    if (helper_ranks_in_uh)
        free(helper_ranks_in_uh);
    return mpi_errno;

  fn_fail:
    goto fn_exit;
}

static int gather_base_offsets(MPI_Aint size, MTCORE_Win * uh_win)
{
    int mpi_errno = MPI_SUCCESS;
    MPI_Aint tmp_u_offsets;
    int i, j;
    int user_local_rank, user_local_nprocs, user_rank, user_nprocs;
    MPI_Aint *base_h_offsets;
    void *base_ptr = NULL;

    PMPI_Comm_rank(uh_win->local_user_comm, &user_local_rank);
    PMPI_Comm_size(uh_win->local_user_comm, &user_local_nprocs);
    PMPI_Comm_rank(uh_win->user_comm, &user_rank);
    PMPI_Comm_size(uh_win->user_comm, &user_nprocs);

    base_h_offsets = calloc(user_nprocs * MTCORE_ENV.num_h, sizeof(MPI_Aint));

#ifdef MTCORE_ENABLE_GRANT_LOCK_HIDDEN_BYTE
    /* All the helpers use the byte located on helper 0. */
    uh_win->grant_lock_h_offset = 0;
#endif

    /* -Calculate the offset of local shared buffer and wait_counter.
     * All wait_counters are on helper 0.  */
    i = 0;
    tmp_u_offsets = 0;
    while (i < user_rank) {
        if (uh_win->targets[i].node_id == uh_win->node_id) {
            tmp_u_offsets += uh_win->targets[i].size;   /* size in bytes */
        }
        i++;
    }

    /* Note that all the helpers start the window from baseptr of helper 0.
     * Hence all the local helpers use the same offset of user buffers */

    /* Calculate the window size of helper 0, because it contains extra space
     * for sync. */
    int root_h_size = MTCORE_HELPER_SHARED_SG_SIZE;
#ifdef MTCORE_ENABLE_GRANT_LOCK_HIDDEN_BYTE
    root_h_size = max(root_h_size, sizeof(MTCORE_GRANT_LOCK_DATATYPE));
#endif

    tmp_u_offsets += root_h_size;
    tmp_u_offsets += MTCORE_HELPER_SHARED_SG_SIZE * (MTCORE_ENV.num_h - 1);

    for (j = 0; j < MTCORE_ENV.num_h; j++) {
        base_h_offsets[user_rank * MTCORE_ENV.num_h + j] = tmp_u_offsets;
    }

    MTCORE_DBG_PRINT("[%d] local base_h_offset 0x%lx\n", user_rank, tmp_u_offsets);

    /* -Receive the address of all the shared user buffers on Helper processes. */
    mpi_errno = PMPI_Allgather(MPI_IN_PLACE, 0, MPI_DATATYPE_NULL, base_h_offsets,
                               MTCORE_ENV.num_h, MPI_AINT, uh_win->user_comm);
    if (mpi_errno != MPI_SUCCESS)
        goto fn_fail;

    for (i = 0; i < user_nprocs; i++) {
        for (j = 0; j < MTCORE_ENV.num_h; j++) {
            uh_win->targets[i].base_h_offsets[j] = base_h_offsets[i * MTCORE_ENV.num_h + j];
            MTCORE_DBG_PRINT("\t.base_h_offsets[%d] = 0x%lx/0x%lx\n",
                             j, uh_win->targets[i].base_h_offsets[j]);
        }
    }

  fn_exit:
    if (base_h_offsets)
        free(base_h_offsets);
    return mpi_errno;

  fn_fail:
    goto fn_exit;
}

static int create_lock_windows(MPI_Aint size, int disp_unit, MPI_Info info, MTCORE_Win * uh_win)
{
    int mpi_errno = MPI_SUCCESS;
    int i, j;
    int user_rank, user_nprocs;

    PMPI_Comm_size(uh_win->user_comm, &user_nprocs);
    PMPI_Comm_rank(uh_win->user_comm, &user_rank);

    /* Need multiple windows for single lock synchronization */
    if (uh_win->info_args.epoch_type & MTCORE_EPOCH_LOCK) {
        uh_win->num_uh_wins = uh_win->max_local_user_nprocs;
    }
    /* Need a single window for lock_all only synchronization */
    else if (uh_win->info_args.epoch_type & MTCORE_EPOCH_LOCK_ALL) {
        uh_win->num_uh_wins = 1;
    }

    uh_win->uh_wins = calloc(uh_win->num_uh_wins, sizeof(MPI_Win));
    for (i = 0; i < uh_win->num_uh_wins; i++) {
        mpi_errno = PMPI_Win_create(uh_win->base, size, disp_unit, info,
                                    uh_win->uh_comm, &uh_win->uh_wins[i]);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;
    }

    for (i = 0; i < user_nprocs; i++) {
        int win_off;
        MTCORE_DBG_PRINT("[%d] targets[%d].\n", user_rank, i);

        /* unique windows of each process, used in lock/flush */
        win_off = uh_win->targets[i].local_user_rank % uh_win->num_uh_wins;
        uh_win->targets[i].uh_win = uh_win->uh_wins[win_off];
        MTCORE_DBG_PRINT("\t\t .uh_win=0x%x (win_off %d)\n", uh_win->targets[i].uh_win, win_off);

        /* windows of each segment, used in OPs */
        for (j = 0; j < uh_win->targets[i].num_segs; j++) {
            win_off = uh_win->targets[i].local_user_rank % uh_win->num_uh_wins;
            uh_win->targets[i].segs[j].uh_win = uh_win->uh_wins[win_off];

            MTCORE_DBG_PRINT("\t\t .seg[%d].uh_win=0x%x (win_off %d)\n",
                             j, uh_win->targets[i].segs[j].uh_win, win_off);
        }
    }

    /* Setup window for local target */
    uh_win->my_uh_win = uh_win->targets[user_rank].segs[0].uh_win;

  fn_exit:
    return mpi_errno;

  fn_fail:
    goto fn_exit;
}

static int clean_up_uh_win(int user_nprocs, MTCORE_Win * uh_win)
{
    int i;

    if (uh_win->local_uh_win && uh_win->local_uh_win != MPI_WIN_NULL)
        PMPI_Win_free(&uh_win->local_uh_win);
    if (uh_win->win && uh_win->win != MPI_WIN_NULL)
        PMPI_Win_free(&uh_win->win);
    if (uh_win->active_win && uh_win->active_win != MPI_WIN_NULL)
        PMPI_Win_free(&uh_win->active_win);
    if (uh_win->num_uh_wins > 0 && uh_win->uh_wins) {
        for (i = 0; i < uh_win->num_uh_wins; i++) {
            if (uh_win->uh_wins[i] && uh_win->uh_wins[i] != MPI_WIN_NULL)
                PMPI_Win_free(&uh_win->uh_wins[i]);
        }
    }

    if (uh_win->ur_h_comm && uh_win->ur_h_comm != MPI_COMM_NULL)
        PMPI_Comm_free(&uh_win->ur_h_comm);
    if (uh_win->local_uh_comm && uh_win->local_uh_comm != MTCORE_COMM_LOCAL)
        PMPI_Comm_free(&uh_win->local_uh_comm);
    if (uh_win->uh_comm && uh_win->uh_comm != MPI_COMM_NULL)
        PMPI_Comm_free(&uh_win->uh_comm);
    if (uh_win->uh_group && uh_win->uh_group != MPI_GROUP_NULL)
        PMPI_Group_free(&uh_win->uh_group);
    if (uh_win->local_user_comm && uh_win->local_user_comm != MTCORE_COMM_USER_LOCAL)
        PMPI_Comm_free(&uh_win->local_user_comm);
    if (uh_win->user_root_comm && uh_win->user_root_comm != MTCORE_COMM_UR_WORLD)
        PMPI_Comm_free(&uh_win->user_root_comm);

    if (uh_win->local_uh_group && uh_win->local_uh_group != MPI_GROUP_NULL)
        PMPI_Group_free(&uh_win->local_uh_group);
    if (uh_win->uh_group && uh_win->uh_group != MPI_GROUP_NULL)
        PMPI_Group_free(&uh_win->uh_group);
    if (uh_win->user_group && uh_win->user_group != MPI_GROUP_NULL)
        PMPI_Group_free(&uh_win->user_group);

#if defined(MTCORE_ENABLE_RUNTIME_LOAD_OPT)
    if (uh_win->h_ops_counts)
        free(uh_win->h_ops_counts);
    if (uh_win->h_bytes_counts)
        free(uh_win->h_bytes_counts);
#endif

    if (uh_win->targets) {
        for (i = 0; i < user_nprocs; i++) {
            if (uh_win->targets[i].base_h_offsets)
                free(uh_win->targets[i].base_h_offsets);
            if (uh_win->targets[i].h_ranks_in_uh)
                free(uh_win->targets[i].h_ranks_in_uh);
            if (uh_win->targets[i].segs)
                free(uh_win->targets[i].segs);
        }
        free(uh_win->targets);
    }
    if (uh_win->h_ranks_in_uh)
        free(uh_win->h_ranks_in_uh);
    if (uh_win->h_win_handles)
        free(uh_win->h_win_handles);
    if (uh_win->uh_wins)
        free(uh_win->uh_wins);
    if (uh_win)
        free(uh_win);
}

int MPI_Win_allocate(MPI_Aint size, int disp_unit, MPI_Info info,
                     MPI_Comm user_comm, void *baseptr, MPI_Win * win)
{
    static const char FCNAME[] = "MPI_Win_allocate";
    int mpi_errno = MPI_SUCCESS;
    MPI_Group uh_group;
    int uh_rank, uh_nprocs, user_nprocs, user_rank, user_world_rank, world_rank,
        user_local_rank, user_local_nprocs, uh_local_rank, uh_local_nprocs;
    MTCORE_Win *uh_win;
    int i, j;
    void **base_pp = (void **) baseptr;
    MPI_Status stat;
    MTCORE_Async_stat my_async_stat = MTCORE_ASYNC_STAT_ON;
    int all_targets_async_off = 1;
    MPI_Aint *tmp_gather_buf = NULL;
    int tmp_gather_cnt = 8;
    int tmp_bcast_buf[2];

    MTCORE_DBG_PRINT_FCNAME();
    MTCORE_RM_TIMER_STR(MTCORE_RM_COMM_TIME);

    uh_win = calloc(1, sizeof(MTCORE_Win));

    mpi_errno = read_win_info(info, uh_win);
    if (mpi_errno != MPI_SUCCESS)
        goto fn_fail;

    /* If user turns off asynchronous redirection, simply return normal window; */
    if (uh_win->info_args.enable_async == 0) {
        if (user_comm == MPI_COMM_WORLD)
            user_comm = MTCORE_COMM_USER_WORLD;

        mpi_errno = PMPI_Win_allocate(size, disp_unit, info, user_comm, baseptr, win);
        MTCORE_DBG_PRINT("User turns off async in win_allocate, return normal win 0x%x\n", *win);

        free(uh_win);
        goto fn_exit;
    }

    /* Only schedule local asynchronous state when user enables auto_async_config
     * and does not force this window to turn on the asynchronous redirection.*/
    if (MTCORE_ENV.auto_async_sched && uh_win->info_args.enable_async != 2) {
        my_async_stat = MTCORE_Sched_my_async_stat();
    }

    /* If user specifies comm_world directly, use user comm_world instead;
     * else this communicator directly, because it should be created from user comm_world */
    if (user_comm == MPI_COMM_WORLD) {
        user_comm = MTCORE_COMM_USER_WORLD;
        uh_win->local_user_comm = MTCORE_COMM_USER_LOCAL;
        uh_win->user_root_comm = MTCORE_COMM_UR_WORLD;

        uh_win->node_id = MTCORE_MY_NODE_ID;
        uh_win->num_nodes = MTCORE_NUM_NODES;
        uh_win->user_comm = user_comm;
    }
    else {
        mpi_errno = PMPI_Comm_split_type(user_comm, MPI_COMM_TYPE_SHARED, 0,
                                         MPI_INFO_NULL, &uh_win->local_user_comm);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;

        uh_win->user_comm = user_comm;

        /* Create a user root communicator in order to figure out node_id and
         * num_nodes of this user communicator */
        PMPI_Comm_rank(uh_win->local_user_comm, &user_local_rank);
        mpi_errno = PMPI_Comm_split(uh_win->user_comm,
                                    user_local_rank == 0, 1, &uh_win->user_root_comm);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;

        if (user_local_rank == 0) {
            int node_id, num_nodes;
            PMPI_Comm_size(uh_win->user_root_comm, &num_nodes);
            PMPI_Comm_rank(uh_win->user_root_comm, &node_id);

            tmp_bcast_buf[0] = node_id;
            tmp_bcast_buf[1] = num_nodes;
        }

        PMPI_Bcast(tmp_bcast_buf, 2, MPI_INT, 0, uh_win->local_user_comm);
        uh_win->node_id = tmp_bcast_buf[0];
        uh_win->num_nodes = tmp_bcast_buf[1];
    }

    PMPI_Comm_group(user_comm, &uh_win->user_group);
    PMPI_Comm_size(user_comm, &user_nprocs);
    PMPI_Comm_rank(user_comm, &user_rank);
    PMPI_Comm_size(uh_win->local_user_comm, &user_local_nprocs);
    PMPI_Comm_rank(uh_win->local_user_comm, &user_local_rank);
    PMPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    PMPI_Comm_rank(MTCORE_COMM_USER_WORLD, &user_world_rank);

    uh_win->h_ranks_in_uh = calloc(MTCORE_ENV.num_h * uh_win->num_nodes, sizeof(MPI_Aint));
    uh_win->targets = calloc(user_nprocs, sizeof(MTCORE_Win_target));
    for (i = 0; i < user_nprocs; i++) {
        uh_win->targets[i].base_h_offsets = calloc(MTCORE_ENV.num_h, sizeof(MPI_Aint));
        uh_win->targets[i].h_ranks_in_uh = calloc(MTCORE_ENV.num_h, sizeof(MPI_Aint));
    }

    /* Gather users' disp_unit, size, ranks and node_id */
    tmp_gather_buf = calloc(user_nprocs * tmp_gather_cnt, sizeof(MPI_Aint));
    tmp_gather_buf[tmp_gather_cnt * user_rank] = (MPI_Aint) disp_unit;
    tmp_gather_buf[tmp_gather_cnt * user_rank + 1] = size;      /* MPI_Aint, size in bytes */
    tmp_gather_buf[tmp_gather_cnt * user_rank + 2] = (MPI_Aint) user_local_rank;
    tmp_gather_buf[tmp_gather_cnt * user_rank + 3] = (MPI_Aint) world_rank;
    tmp_gather_buf[tmp_gather_cnt * user_rank + 4] = (MPI_Aint) user_world_rank;
    tmp_gather_buf[tmp_gather_cnt * user_rank + 5] = (MPI_Aint) uh_win->node_id;
    tmp_gather_buf[tmp_gather_cnt * user_rank + 6] = (MPI_Aint) user_local_nprocs;
    tmp_gather_buf[tmp_gather_cnt * user_rank + 7] = (MPI_Aint) my_async_stat;

    mpi_errno = PMPI_Allgather(MPI_IN_PLACE, 0, MPI_DATATYPE_NULL,
                               tmp_gather_buf, tmp_gather_cnt, MPI_AINT, user_comm);
    if (mpi_errno != MPI_SUCCESS)
        goto fn_fail;
    for (i = 0; i < user_nprocs; i++) {
        uh_win->targets[i].disp_unit = (int) tmp_gather_buf[tmp_gather_cnt * i];
        uh_win->targets[i].size = tmp_gather_buf[tmp_gather_cnt * i + 1];
        uh_win->targets[i].local_user_rank = (int) tmp_gather_buf[tmp_gather_cnt * i + 2];
        uh_win->targets[i].world_rank = (int) tmp_gather_buf[tmp_gather_cnt * i + 3];
        uh_win->targets[i].user_world_rank = (int) tmp_gather_buf[tmp_gather_cnt * i + 4];
        uh_win->targets[i].node_id = (int) tmp_gather_buf[tmp_gather_cnt * i + 5];
        uh_win->targets[i].local_user_nprocs = (int) tmp_gather_buf[tmp_gather_cnt * i + 6];
        uh_win->targets[i].async_stat = (MTCORE_Async_stat) tmp_gather_buf[tmp_gather_cnt
                                                                           * user_rank + 7];
        all_targets_async_off &= (uh_win->targets[i].async_stat == MTCORE_ASYNC_STAT_OFF);

        /* Calculate the maximum number of processes per node */
        uh_win->max_local_user_nprocs = max(uh_win->max_local_user_nprocs,
                                            uh_win->targets[i].local_user_nprocs);
    }

#ifdef DEBUG
    MTCORE_DBG_PRINT("my user local rank %d/%d, max_local_user_nprocs=%d, num_nodes=%d\n",
                     user_local_rank, user_local_nprocs, uh_win->max_local_user_nprocs,
                     uh_win->num_nodes);
    for (i = 0; i < user_nprocs; i++) {
        MTCORE_DBG_PRINT("\t targets[%d].disp_unit=%d, size=%ld, local_user_rank=%d, "
                         "world_rank=%d, user_world_rank=%d, node_id=%d, local_user_nprocs=%d\n",
                         i, uh_win->targets[i].disp_unit, uh_win->targets[i].size,
                         uh_win->targets[i].local_user_rank, uh_win->targets[i].world_rank,
                         uh_win->targets[i].user_world_rank, uh_win->targets[i].node_id,
                         uh_win->targets[i].local_user_nprocs);
    }
#endif

    /* If the asynchronous states on all processes are off, simply return normal window; */
    if (all_targets_async_off) {
        mpi_errno = PMPI_Win_allocate(size, disp_unit, info, user_comm, baseptr, win);
        MTCORE_DBG_PRINT("All async off in win_allocate, return normal win 0x%x\n", *win);

        clean_up_uh_win(user_nprocs, uh_win);
        goto fn_exit;
    }

    /* Notify Helpers start and create user root + helpers communicator for
     * internal information exchange between users and helpers. */
    if (user_local_rank == 0) {
        mpi_errno = MTCORE_Func_start(MTCORE_FUNC_WIN_ALLOCATE, user_nprocs, user_local_nprocs);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;

        mpi_errno = MTCORE_Func_new_ur_h_comm(&uh_win->ur_h_comm);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;
    }

    /* Create communicators
     *  uh_comm: including all USER and Helper processes
     *  local_uh_comm: including local USER and Helper processes
     */
    mpi_errno = create_communicators(uh_win);
    if (mpi_errno != MPI_SUCCESS)
        goto fn_fail;

    PMPI_Comm_rank(uh_win->local_uh_comm, &uh_local_rank);
    PMPI_Comm_size(uh_win->local_uh_comm, &uh_local_nprocs);
    PMPI_Comm_size(uh_win->uh_comm, &uh_nprocs);
    PMPI_Comm_rank(uh_win->uh_comm, &uh_rank);
    PMPI_Comm_rank(uh_win->uh_comm, &uh_win->my_rank_in_uh_comm);
    MTCORE_DBG_PRINT(" Created uh_comm: %d/%d, local_uh_comm: %d/%d\n",
                     uh_rank, uh_nprocs, uh_local_rank, uh_local_nprocs);

#if defined(MTCORE_ENABLE_RUNTIME_LOAD_OPT)
    uh_win->h_ops_counts = calloc(uh_nprocs, sizeof(int));
    uh_win->h_bytes_counts = calloc(uh_nprocs, sizeof(unsigned long));
#endif

    /* Allocate a shared window with local Helpers */
    mpi_errno = PMPI_Win_allocate_shared(size, disp_unit, info, uh_win->local_uh_comm,
                                         &uh_win->base, &uh_win->local_uh_win);
    if (mpi_errno != MPI_SUCCESS)
        goto fn_fail;
    MTCORE_DBG_PRINT("[%d] allocate shared base = %p\n", user_rank, uh_win->base);

    /* Gather user offsets on corresponding helper processes */
    mpi_errno = gather_base_offsets(size, uh_win);
    if (mpi_errno != MPI_SUCCESS)
        goto fn_fail;

    specify_main_helper_binding(uh_win);

    /* Create windows using shared buffers. */

    /* Send information to helpers */
    if (user_local_rank == 0) {
        int func_params[2];
        func_params[0] = uh_win->max_local_user_nprocs;
        func_params[1] = uh_win->info_args.epoch_type;
        mpi_errno = MTCORE_Func_set_param((char *) func_params, sizeof(func_params),
                                          uh_win->ur_h_comm);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;
        MTCORE_DBG_PRINT(" Send parameters: max_local_user_nprocs %d, epoch_type %d \n",
                         uh_win->max_local_user_nprocs, uh_win->info_args.epoch_type);
    }

    if ((uh_win->info_args.epoch_type & MTCORE_EPOCH_LOCK) ||
        (uh_win->info_args.epoch_type & MTCORE_EPOCH_LOCK_ALL)) {

        mpi_errno = create_lock_windows(size, disp_unit, info, uh_win);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;
    }

    /* - Create global active window */
    if ((uh_win->info_args.epoch_type & MTCORE_EPOCH_FENCE) ||
        (uh_win->info_args.epoch_type & MTCORE_EPOCH_PSCW)) {
        mpi_errno = PMPI_Win_create(uh_win->base, size, disp_unit, info,
                                    uh_win->uh_comm, &uh_win->active_win);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;

        MTCORE_DBG_PRINT("[%d] Created active window 0x%x\n", user_rank, uh_win->active_win);

        /* Since all processes must be in win_allocate, we do not need worry
         * the possibility losing asynchronous progress.
         * This lock_all guarantees the semantics correctness when internally
         * change to passive mode. */
        mpi_errno = PMPI_Win_lock_all(MPI_MODE_NOCHECK, uh_win->active_win);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;
        uh_win->start_counter = 0;
    }

    /* Track epoch status for redirecting RMA to different window. */
    uh_win->epoch_stat = MTCORE_WIN_NO_EPOCH;

    /* - Only expose user window in order to hide helpers in all non-wrapped window functions */
    mpi_errno = PMPI_Win_create(uh_win->base, size, disp_unit, info,
                                uh_win->user_comm, &uh_win->win);
    if (mpi_errno != MPI_SUCCESS)
        goto fn_fail;

    MTCORE_DBG_PRINT("[%d] Created window 0x%x\n", user_rank, uh_win->win);

    *win = uh_win->win;
    *base_pp = uh_win->base;

    /* Gather the handle of Helpers' win. User root is always rank num_h in user
     * root + helpers communicator */
    /* TODO:
     * How about use handler on user root ?
     * How to solve the case that different processes may have the same handler ? */
    if (user_local_rank == 0) {
        unsigned long tmp_send_buf;
        uh_win->h_win_handles = calloc(MTCORE_ENV.num_h + 1, sizeof(unsigned long));
        mpi_errno = PMPI_Gather(&tmp_send_buf, 1, MPI_UNSIGNED_LONG, uh_win->h_win_handles,
                                1, MPI_UNSIGNED_LONG, MTCORE_ENV.num_h, uh_win->ur_h_comm);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;
    }

    MTCORE_Cache_uh_win(uh_win->win, uh_win);

  fn_exit:
    MTCORE_RM_TIMER_END(MTCORE_RM_COMM_TIME);
    if (tmp_gather_buf)
        free(tmp_gather_buf);

    return mpi_errno;

  fn_fail:

    /* Caching is the last possible error, so we do not need remove
     * cache here. */
    clean_up_uh_win(user_nprocs, uh_win);

    *win = MPI_WIN_NULL;
    *base_pp = NULL;

    goto fn_exit;
}
