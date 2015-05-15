#ifndef MTCORE_H_
#define MTCORE_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

#define MTCORE_ENABLE_GRANT_LOCK_HIDDEN_BYTE

/* Enable local lock opt by default, unless disable it explicitly */
#ifndef MTCORE_DISABLE_LOCAL_LOCK_OPT
#define MTCORE_ENABLE_LOCAL_LOCK_OPT    /* Optimization for local target.
                                         * Lock/RMA/Flush/Unlock local target instead of helpers.
                                         * Only available when local lock is granted. */
#endif

#ifdef MTCORE_ENABLE_GRANT_LOCK_HIDDEN_BYTE
#define MTCORE_GRANT_LOCK_DATATYPE char
#define MTCORE_GRANT_LOCK_MPI_DATATYPE MPI_CHAR
#endif

#define MTCORE_SEGMENT_UNIT 16

#define MTCORE_PSCW_CW_TAG 900
#define MTCORE_PSCW_PS_TAG 901

/*FIXME: It is a workaround for shared window overlapping problem
 * when shared segment size of each helper is 0 */
#define MTCORE_HELPER_SHARED_SG_SIZE 4096

/* Options for lock permission controlling among multiple helpers.
 *
 * Since RMA Ops to a given target may be distributed to different helpers
 * and locks will be guaranteed to be acquired only when an Op happens,
 * two origins may access a target concurrently if their Ops are distributed
 * to different helpers.
 *
 *  Rank binding:
 *      Statically specify single helper for each target, thus real locks/Ops
 *      to a given target will only be issued to the same helper.
 *
 *  Segment binding:
 *      Statically specify single helper for each segment of shared memory,
 *      thus real locks/Ops to a given byte will only be issued to the same
 *      helper. Consequently, it eliminates the case that two origins concurrently
 *      access the same address of a target.
 *      This method has additional overhead especially for derived target datatype.
 *      But it is more fine-grained than Rank binding.
 * */

#ifdef HAVE_BUILTIN_EXPECT
#  define unlikely(x_) __builtin_expect(!!(x_),0)
#  define likely(x_)   __builtin_expect(!!(x_),1)
#else
#  define unlikely(x_) (x_)
#  define likely(x_)   (x_)
#endif

#ifdef DEBUG
#define MTCORE_DBG_PRINT(str,...) do { \
    fprintf(stdout, "[MTCORE][%d]"str, MTCORE_MY_RANK_IN_WORLD, ## __VA_ARGS__); \
    fflush(stdout); \
    } while (0)
#else
#define MTCORE_DBG_PRINT(str,...) {}
#endif

#define WARN
#ifdef WARN
#define MTCORE_WARN_PRINT(str,...) do { \
    fprintf(stdout, "[MTCORE][%d]"str, MTCORE_MY_RANK_IN_WORLD, ## __VA_ARGS__); \
    fflush(stdout); \
    } while (0)
#else
#define MTCORE_WARN_PRINT(str,...) {}
#endif

#define MTCORE_DBG_PRINT_FCNAME() MTCORE_DBG_PRINT("in %s\n", __FUNCTION__)
#define MTCORE_ERR_PRINT(str,...) do { \
    fprintf(stderr, "[%d]"str, MTCORE_MY_RANK_IN_WORLD, ## __VA_ARGS__); \
    fflush(stdout); \
    } while (0)

#define MTCORE_Assert(EXPR) do { if (unlikely(!(EXPR))){ \
            MTCORE_ERR_PRINT("[MTCORE][N-%d, %d]  assert fail in [%s:%d]: \"%s\"\n", \
                    MTCORE_MY_NODE_ID, MTCORE_MY_RANK_IN_WORLD, __FILE__, __LINE__, #EXPR); \
            PMPI_Abort(MPI_COMM_WORLD, -1); \
        }} while (0)


#ifndef max
#define max(a,b) \
    ({ typeof (a) _a = (a); \
       typeof (b) _b = (b); \
       _a > _b ? _a : _b; })
#endif

#ifndef min
#define min(a,b) \
    ({ typeof (a) _a = (a); \
       typeof (b) _b = (b); \
       _a < _b ? _a : _b; })
#endif

#ifndef align
#define align(val, align) (((val) + (align) - 1) & ~((align) - 1))
#endif

typedef enum {
    MTCORE_LOAD_OPT_STATIC,
    MTCORE_LOAD_OPT_RANDOM,
    MTCORE_LOAD_OPT_COUNTING,
    MTCORE_LOAD_BYTE_COUNTING,
} MTCORE_Load_opt;

typedef enum {
    MTCORE_LOAD_LOCK_NATURE,
    MTCORE_LOAD_LOCK_FORCE,
} MTCORE_Load_lock;

typedef enum {
    MTCORE_LOCK_BINDING_RANK,
    MTCORE_LOCK_BINDING_SEGMENT,
} MTCORE_Lock_binding;

#define MTCORE_DEFAULT_SEG_SIZE 4096;
#define MTCORE_DEFAULT_NUM_HELPER 1

typedef struct MTCORE_Env_param {
    int num_h;
    int seg_size;               /* segment size in lock segment binding */
    MTCORE_Load_opt load_opt;   /* runtime load balancing options */
    MTCORE_Load_lock load_lock; /* how to grant locks for runtime load balancing */
    MTCORE_Lock_binding lock_binding;   /* how to handle locks */
    int auto_async_sched;       /* automatic asynchronous progress configuration, false by default */
    long long auto_async_sched_thr_h;
    long long auto_async_sched_thr_l;
} MTCORE_Env_param;


/* used in runtime load balancing */
typedef enum {
    MTCORE_MAIN_LOCK_RESET,
    MTCORE_MAIN_LOCK_OP_ISSUED,
    MTCORE_MAIN_LOCK_GRANTED
} MTCORE_Main_lock_stat;

typedef enum {
    MTCORE_WIN_NO_EPOCH,
    MTCORE_WIN_EPOCH_FENCE,
    MTCORE_WIN_EPOCH_LOCK,
    MTCORE_WIN_EPOCH_PSCW,
} MTCORE_Win_epoch_stat;

typedef enum {
    MTCORE_FUNC_NULL,
    MTCORE_FUNC_WIN_ALLOCATE,
    MTCORE_FUNC_WIN_FREE,
    MTCORE_FUNC_LOCL_ALL,
    MTCORE_FUNC_UNLOCK_ALL,
    MTCORE_FUNC_ABORT,
    MTCORE_FUNC_FINALIZE,
    MTCORE_FUNC_MAX,
} MTCORE_Func;

typedef enum {
    MTCORE_EPOCH_LOCK_ALL = 1,
    MTCORE_EPOCH_LOCK = 2,
    MTCORE_EPOCH_PSCW = 4,
    MTCORE_EPOCH_FENCE = 8,
} MTCORE_Epoch_type;

typedef enum {
    MTCORE_ASYNC_STAT_ON = 0,
    MTCORE_ASYNC_STAT_OFF = 1
} MTCORE_Async_stat;

typedef struct MTCORE_H_win_params {
    MPI_Aint size;
    int disp_unit;
} MTCORE_H_win_params;

struct MTCORE_Win_info_args {
    unsigned short no_local_load_store;
    int epoch_type;
    int enable_async;           /* 0,1,2|0:off; 1:default on (default); 2:force on */
};

typedef struct MTCORE_OP_Segment {
    void *origin_addr;
    int origin_count;
    MPI_Datatype origin_datatype;

    int target_rank;
    int target_seg_off;
    MPI_Aint target_disp;
    int target_count;
    int target_dtsize;
    MPI_Datatype target_datatype;

} MTCORE_OP_Segment;

typedef struct MTCORE_Win_target_seg {
    MPI_Aint base_offset;
    int size;

    int main_h_off;
    MPI_Win uh_win;

#if defined(MTCORE_ENABLE_RUNTIME_LOAD_OPT)
    MTCORE_Main_lock_stat main_lock_stat;
#endif
} MTCORE_Win_target_seg;

typedef struct MTCORE_Win_target {
    MPI_Win uh_win;             /* Do not free the window, it is freed in uh_wins */
    int disp_unit;
    MPI_Aint size;

    MPI_Aint *base_h_offsets;   /* MTCORE_ENV.num_h */
    int *h_ranks_in_uh;         /* MTCORE_ENV.num_h */
    int remote_lock_assert;

    int local_user_rank;        /* rank in local user communicator */
    int local_user_nprocs;
    int world_rank;             /* rank in world communicator */
    int user_world_rank;        /* rank in user world communicator */
    int uh_rank;
    int node_id;

    MPI_Aint wait_counter_offset;       /* counter for complete-wait synchronization. allocated in main helper. */
    MPI_Aint post_flg_offset;   /* flag for post-start synchronization. allocated in main helper. */

    /* Only contain 1 segment in rank binding */
    MTCORE_Win_target_seg *segs;
    int num_segs;

    MTCORE_Async_stat async_stat;
} MTCORE_Win_target;

typedef struct MTCORE_Win {
    /* communicator including root user processes and all helpers,
     * used for internal information exchange between users and helpers */
    MPI_Comm ur_h_comm;

    /* communicator including local process and helpers */
    MPI_Comm local_uh_comm;
    MPI_Group local_uh_group;
    MPI_Win local_uh_win;

    int num_h_ranks_in_uh;      /* number of unique helper ranks */
    int *h_ranks_in_uh;         /* unique helper ranks in world, used in lockall only epoches. */
    int my_rank_in_uh_comm;     /* remember my rank in internal uh_comm for local RMA. Specified in win_allocate. */
    MPI_Win my_uh_win;          /* Do not free the window, it is referred from another window. Specified in win_allocate. */
    unsigned short is_self_locked;

    /* communicator including all the user processes and helpers */
    MPI_Comm uh_comm;
    MPI_Group uh_group;
    MPI_Win *uh_wins;           /* every local process has separate window for permission control,
                                 * processes in different node share one window. */
    int num_uh_wins;            /* = max_local_user_nprocs */

    /* communicator including all the user processes */
    MPI_Comm user_comm;
    MPI_Group user_group;
    MPI_Comm user_root_comm;

    MPI_Comm local_user_comm;
    int max_local_user_nprocs;
    int num_nodes;
    int node_id;

    MTCORE_Win_epoch_stat epoch_stat;   /* indicate which epoch is opened. thus operations
                                         * can send to the correct window. Note that only
                                         * change from lock to NO_EPOCH when lock counter is
                                         * equal to 0, otherwise the whole window is still in
                                         * LOCK epoch */
    int lock_counter;
    int lockall_counter;

    MPI_Win active_win;

    MPI_Group start_group;
    MPI_Group post_group;
    int *start_ranks_in_win_group;
    int *post_ranks_in_win_group;
    int start_counter;

    void *base;
    MPI_Win win;
    MTCORE_Win_target *targets;

    unsigned long *h_win_handles;

#ifdef MTCORE_ENABLE_GRANT_LOCK_HIDDEN_BYTE
    MPI_Aint grant_lock_h_offset;       /* Hidden byte for granting lock on Helper0 */
#endif

    struct MTCORE_Win_info_args info_args;

#if defined(MTCORE_ENABLE_RUNTIME_LOAD_OPT)
    int prev_h_off;
    int *h_ops_counts;          /* cnt = h_ops_counts[h_rank_in_uh] */
    unsigned long *h_bytes_counts;      /* byte = h_ops_bytes[h_rank_in_uh] */
#endif

} MTCORE_Win;

typedef struct MTCORE_Func_info {
    MTCORE_Func FUNC;
    int user_nprocs;
    int user_local_nprocs;
} MTCORE_Func_info;

#define MTCORE_FUNC_TAG 9889

#define MTCORE_Define_win_cache int UH_WIN_HANDLE_KEY = MPI_KEYVAL_INVALID
extern int UH_WIN_HANDLE_KEY;

#define MTCORE_Init_win_cache() {    \
    mpi_errno = PMPI_Win_create_keyval(MPI_WIN_NULL_COPY_FN, \
            MPI_WIN_NULL_DELETE_FN, &UH_WIN_HANDLE_KEY, (void *) 0);    \
    if (mpi_errno != 0) \
        goto fn_fail;   \
}

#define MTCORE_Destroy_win_cache() {    \
    if (UH_WIN_HANDLE_KEY != MPI_KEYVAL_INVALID) {  \
        mpi_errno = PMPI_Win_free_keyval(&UH_WIN_HANDLE_KEY);    \
        if (mpi_errno != MPI_SUCCESS){  \
            MTCORE_ERR_PRINT("Free UH_WIN_HANDLE_KEY %p\n", &UH_WIN_HANDLE_KEY);   \
        }   /*Do not jump to fn_fail, because it is also used in fn_fail processing */ \
    }   \
}

#define MTCORE_Fetch_uh_win_from_cache(win, uh_win) { \
    int flag = 0;   \
    mpi_errno = PMPI_Win_get_attr(win, UH_WIN_HANDLE_KEY, &uh_win, &flag);   \
    if (!flag || mpi_errno != MPI_SUCCESS){  \
        MTCORE_DBG_PRINT("Cannot fetch uh_win from win 0x%x\n", win);   \
        uh_win = NULL; \
    }   \
}

#define MTCORE_Cache_uh_win(win, uh_win) { \
    mpi_errno = PMPI_Win_set_attr(win, UH_WIN_HANDLE_KEY, uh_win);  \
    if (mpi_errno != MPI_SUCCESS){  \
        MTCORE_ERR_PRINT("Cannot cache uh_win %p for win 0x%x\n", uh_win, win);   \
        goto fn_fail;   \
    }   \
    MTCORE_DBG_PRINT("cache uh_win %p into win 0x%x \n", uh_win, win);  \
}

#define MTCORE_Remove_uh_win_from_cache(win)  {\
    mpi_errno = PMPI_Win_delete_attr(win, UH_WIN_HANDLE_KEY);   \
    if (mpi_errno != MPI_SUCCESS){  \
        MTCORE_ERR_PRINT("Cannot remove uh_win cache for win 0x%x\n", win);   \
        goto fn_fail;   \
    }   \
}

extern MPI_Comm MTCORE_COMM_USER_WORLD;
extern MPI_Comm MTCORE_COMM_LOCAL;
extern MPI_Comm MTCORE_COMM_USER_LOCAL;
extern MPI_Comm MTCORE_COMM_UR_WORLD;
extern MPI_Comm MTCORE_COMM_HELPER_LOCAL;
extern MPI_Group MTCORE_GROUP_WORLD;
extern MPI_Group MTCORE_GROUP_LOCAL;
extern MPI_Group MTCORE_GROUP_USER_WORLD;

extern int *MTCORE_H_RANKS_IN_WORLD;
extern int *MTCORE_H_RANKS_IN_LOCAL;
extern int *MTCORE_ALL_H_RANKS_IN_WORLD;
extern int *MTCORE_ALL_UNIQUE_H_RANKS_IN_WORLD;
extern int *MTCORE_USER_RANKS_IN_WORLD;
extern int MTCORE_NUM_NODES;
extern int MTCORE_MY_NODE_ID;
extern int *MTCORE_ALL_NODE_IDS;
extern int MTCORE_MY_RANK_IN_WORLD;

extern MTCORE_Env_param MTCORE_ENV;

static inline int MTCORE_Get_node_ids(MPI_Group group, int n, const int ranks[], int node_ids[])
{
    int mpi_errno = MPI_SUCCESS;
    int *ranks_in_world = NULL;
    int i;

    if (n == 0)
        return mpi_errno;

    ranks_in_world = calloc(n, sizeof(int));

    mpi_errno = PMPI_Group_translate_ranks(group, n, ranks, MTCORE_GROUP_WORLD, ranks_in_world);
    if (mpi_errno != MPI_SUCCESS)
        goto fn_fail;

    for (i = 0; i < n; i++) {
        node_ids[i] = MTCORE_ALL_NODE_IDS[ranks_in_world[i]];
    }

  fn_exit:
    if (ranks_in_world)
        free(ranks_in_world);
    return mpi_errno;

  fn_fail:
    goto fn_exit;
}

#if defined(MTCORE_ENABLE_RUNTIME_LOAD_OPT)
#define MTCORE_Reset_win_target_load_opt_op_counting(target_rank, uh_win) {  \
        int h_off, h_rank;  \
        for (h_off = 0; h_off < MTCORE_ENV.num_h; h_off++) {    \
            h_rank = uh_win->targets[target_rank].h_ranks_in_uh[h_off]; \
            uh_win->h_ops_counts[h_rank] = 0;    \
        }   \
        MTCORE_DBG_PRINT("[load_opt_op] reset target %d op counting \n", target_rank); \
    }

#define MTCORE_Reset_win_target_load_opt_bytes_counting(target_rank, uh_win) {  \
        int h_off, h_rank;  \
        for (h_off = 0; h_off < MTCORE_ENV.num_h; h_off++) {    \
            h_rank = uh_win->targets[target_rank].h_ranks_in_uh[h_off]; \
            uh_win->h_bytes_counts[h_rank] = 0;    \
        }   \
        MTCORE_DBG_PRINT("[load_opt_byte] reset target %d byte counting \n", target_rank); \
    }

#define MTCORE_Reset_win_target_load_opt(target_rank, uh_win) { \
        if (MTCORE_ENV.load_opt == MTCORE_LOAD_OPT_COUNTING){ \
            MTCORE_Reset_win_target_load_opt_op_counting(target_rank, uh_win) ; \
        } else if (MTCORE_ENV.load_opt == MTCORE_LOAD_BYTE_COUNTING){  \
            MTCORE_Reset_win_target_load_opt_bytes_counting(target_rank, uh_win) ; \
        }   \
    }


#define MTCORE_Inc_win_target_load_opt_op_counting(h_rank_in_uh, uh_win) {  \
        uh_win->h_ops_counts[h_rank_in_uh]++;   \
        MTCORE_DBG_PRINT("[load_opt_op] increment helper %d\n", h_rank_in_uh); \
    }

#define MTCORE_Inc_win_target_load_opt_bytes_counting(h_rank_in_uh, size, uh_win) {  \
        uh_win->h_bytes_counts[h_rank_in_uh] += size;   \
        MTCORE_DBG_PRINT("[load_opt_byte] increment helper %d\n", h_rank_in_uh); \
    }
#endif

static inline int MTCORE_Is_in_shrd_mem(int target_rank, MPI_Group group, int *node_id,
                                        int *is_shared)
{
    int mpi_errno = MPI_SUCCESS;
    int target_node_id = -1;
    *is_shared = 0;

    /* If target is in the same node, use shared window instead */
    mpi_errno = MTCORE_Get_node_ids(group, 1, &target_rank, &target_node_id);
    if (mpi_errno != MPI_SUCCESS)
        return mpi_errno;

    if (target_node_id == MTCORE_ALL_NODE_IDS[MTCORE_MY_RANK_IN_WORLD]) {
        *is_shared = 1;
    }

    *node_id = target_node_id;

    return mpi_errno;
}

static inline int MTCORE_Win_grant_local_lock(int target_rank, int lock_type,
                                              int assert, MTCORE_Win * uh_win)
{
    int mpi_errno = MPI_SUCCESS;
    int user_rank, j;

    PMPI_Comm_rank(uh_win->user_comm, &user_rank);

    /* force lock all the main helpers for each segment */
    for (j = 0; j < uh_win->targets[target_rank].num_segs; j++) {
        int main_h_off = uh_win->targets[target_rank].segs[j].main_h_off;
        int target_h_rank_in_uh = uh_win->targets[target_rank].h_ranks_in_uh[main_h_off];

#ifdef MTCORE_ENABLE_GRANT_LOCK_HIDDEN_BYTE
        MTCORE_GRANT_LOCK_DATATYPE buf[1];
        mpi_errno = PMPI_Get(buf, 1, MTCORE_GRANT_LOCK_MPI_DATATYPE, target_h_rank_in_uh,
                             uh_win->grant_lock_h_offset, 1, MTCORE_GRANT_LOCK_MPI_DATATYPE,
                             uh_win->targets[target_rank].segs[j].uh_win);
#else
        /* Simply get 1 byte from start, it does not affect the result of other updates */
        char buf[1];
        mpi_errno = PMPI_Get(buf, 1, MPI_CHAR, target_h_rank_in_uh, 0,
                             1, MPI_CHAR, uh_win->targets[user_rank].segs[j].uh_win);
#endif
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;

        mpi_errno = PMPI_Win_flush(target_h_rank_in_uh,
                                   uh_win->targets[target_rank].segs[j].uh_win);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;

#if defined(MTCORE_ENABLE_RUNTIME_LOAD_OPT)
        uh_win->targets[target_rank].segs[j].main_lock_stat = MTCORE_MAIN_LOCK_GRANTED;
#endif
        MTCORE_DBG_PRINT("[%d]grant local lock(Helper(%d), uh_wins 0x%x) seg %d\n", user_rank,
                         target_h_rank_in_uh, uh_win->targets[target_rank].segs[j].uh_win, j);

    }

  fn_exit:
    return mpi_errno;

  fn_fail:
    goto fn_exit;
}

extern const char *MTCORE_Win_epoch_stat_name[4];       /* for debug */

#define MTCORE_Get_epoch_local_win(uh_win, win_ptr) { \
    switch (uh_win->epoch_stat) {   \
        case MTCORE_WIN_EPOCH_FENCE:    \
        case MTCORE_WIN_EPOCH_PSCW: \
            win_ptr = &uh_win->active_win;   \
            break;  \
        default:    \
            win_ptr = &uh_win->my_uh_win;   \
            break;  \
    }   \
}

#define MTCORE_Get_epoch_win(target_rank, seg, uh_win, win_ptr) { \
    switch (uh_win->epoch_stat) {   \
        case MTCORE_WIN_EPOCH_FENCE:    \
        case MTCORE_WIN_EPOCH_PSCW: \
            win_ptr = &uh_win->active_win;   \
            break;  \
        default:    \
            win_ptr = &uh_win->targets[target_rank].segs[seg].uh_win;   \
            break;  \
    }   \
}

extern int run_h_main(void);

extern int MTCORE_Func_start(MTCORE_Func FUNC, int user_nprocs, int user_local_nprocs);
extern int MTCORE_Func_new_ur_h_comm(MPI_Comm * ur_h_comm);
extern int MTCORE_Func_set_param(char *func_params, int size, MPI_Comm ur_h_comm);


#if defined(MTCORE_ENABLE_RUNTIME_LOAD_OPT)

static inline int MTCORE_Win_grant_lock(int target_rank, int target_seg_off, MTCORE_Win * uh_win)
{
    int mpi_errno = MPI_SUCCESS;
    int main_h_off = uh_win->targets[target_rank].segs[target_seg_off].main_h_off;

    mpi_errno = PMPI_Win_flush(uh_win->targets[target_rank].h_ranks_in_uh[main_h_off],
                               uh_win->targets[target_rank].segs[target_seg_off].uh_win);
    if (mpi_errno == MPI_SUCCESS) {
        uh_win->targets[target_rank].segs[target_seg_off].main_lock_stat = MTCORE_MAIN_LOCK_GRANTED;

        MTCORE_DBG_PRINT("grant lock(Helper(%d), uh_wins 0x%x) for target %d seg %d\n",
                         uh_win->targets[target_rank].h_ranks_in_uh[main_h_off],
                         uh_win->targets[target_rank].segs[target_seg_off].uh_win,
                         target_rank, target_seg_off);
    }

    return mpi_errno;
}

static inline void MTCORE_Get_helper_rank_load_opt_random(int target_rank, int is_order_required,
                                                          MTCORE_Win * uh_win,
                                                          int *target_h_rank_in_uh,
                                                          int *target_h_rank_idx,
                                                          MPI_Aint * target_h_offset)
{
    /* Randomly change helper offset every time using a window-level global recorder */
    int idx = (uh_win->prev_h_off + 1) % MTCORE_ENV.num_h;      /* jump to next helper offset */
    uh_win->prev_h_off = idx;

    *target_h_rank_in_uh = uh_win->targets[target_rank].h_ranks_in_uh[idx];
    *target_h_offset = uh_win->targets[target_rank].base_h_offsets[idx];
    *target_h_rank_idx = idx;

    MTCORE_DBG_PRINT("[load_opt_random] randomly choose helper %d, off 0x%lx for target %d\n",
                     *target_h_rank_in_uh, *target_h_offset, target_rank);

}

extern void MTCORE_Get_helper_rank_load_opt_counting(int target_rank, int is_order_required,
                                                     MTCORE_Win * uh_win, int *target_h_rank_in_uh,
                                                     int *target_h_rank_idx,
                                                     MPI_Aint * target_h_offset);
extern void MTCORE_Get_helper_rank_load_byte_counting(int target_rank, int is_order_required,
                                                      int size, MTCORE_Win * uh_win,
                                                      int *target_h_rank_in_uh,
                                                      int *target_h_rank_idx,
                                                      MPI_Aint * target_h_offset);

static inline int MTCORE_Get_helper_rank_load_opt(int target_rank, int target_seg_off,
                                                  int is_order_required,
                                                  int size, MTCORE_Win * uh_win,
                                                  int *target_h_rank_in_uh,
                                                  MPI_Aint * target_h_offset)
{
    int mpi_errno = MPI_SUCCESS;
    int main_h_off = uh_win->targets[target_rank].segs[target_seg_off].main_h_off;
    int h_idx = 0;

    /* Force lock when the first operation is issued. Note that nocheck epoch
     * does not need it because no conflicting lock.*/
    if (MTCORE_ENV.load_lock == MTCORE_LOAD_LOCK_FORCE &&
        !(uh_win->targets[target_rank].remote_lock_assert & MPI_MODE_NOCHECK) &&
        uh_win->targets[target_rank].segs[target_seg_off].main_lock_stat ==
        MTCORE_MAIN_LOCK_OP_ISSUED) {
        mpi_errno = MTCORE_Win_grant_lock(target_rank, target_seg_off, uh_win);
        if (mpi_errno != MPI_SUCCESS)
            return mpi_errno;
    }

    /* Upgrade main lock status of target if it is the first operation of that target. */
    if (uh_win->targets[target_rank].segs[target_seg_off].main_lock_stat == MTCORE_MAIN_LOCK_RESET) {
        uh_win->targets[target_rank].segs[target_seg_off].main_lock_stat =
            MTCORE_MAIN_LOCK_OP_ISSUED;
    }

    /* If lock has not been granted yet, we can only use the main helper.
     * Accumulate operations have to be always sent to main helper in order to
     * guarantee atomicity and ordering.*/
    if ((!(uh_win->targets[target_rank].remote_lock_assert & MPI_MODE_NOCHECK) &&
         uh_win->targets[target_rank].segs[target_seg_off].main_lock_stat !=
         MTCORE_MAIN_LOCK_GRANTED) || is_order_required) {
        /* Both serial async and byte tracking options specify the first helper as
         * the main helper of that user process.*/
        *target_h_rank_in_uh = uh_win->targets[target_rank].h_ranks_in_uh[main_h_off];
        *target_h_offset = uh_win->targets[target_rank].base_h_offsets[main_h_off];
        MTCORE_DBG_PRINT("[load_opt] use main helper %d, off 0x%lx for target %d "
                         "seg %d (main h off %d)\n",
                         *target_h_rank_in_uh, *target_h_offset, target_rank,
                         target_seg_off, main_h_off);

        /* Need increase counters */
        if (MTCORE_ENV.load_opt == MTCORE_LOAD_OPT_COUNTING) {
            MTCORE_Inc_win_target_load_opt_op_counting(*target_h_rank_in_uh, uh_win);
        }
        else if (MTCORE_ENV.load_opt == MTCORE_LOAD_BYTE_COUNTING) {
            MTCORE_Inc_win_target_load_opt_bytes_counting(*target_h_rank_in_uh, size, uh_win);
        }

        return mpi_errno;
    }

    /* Runtime load balancing */
    if (MTCORE_ENV.load_opt == MTCORE_LOAD_OPT_RANDOM) {
        MTCORE_Get_helper_rank_load_opt_random(target_rank, is_order_required, uh_win,
                                               target_h_rank_in_uh, &h_idx, target_h_offset);
    }
    else if (MTCORE_ENV.load_opt == MTCORE_LOAD_OPT_COUNTING) {
        MTCORE_Get_helper_rank_load_opt_counting(target_rank, is_order_required, uh_win,
                                                 target_h_rank_in_uh, &h_idx, target_h_offset);
    }
    else if (MTCORE_ENV.load_opt == MTCORE_LOAD_BYTE_COUNTING) {
        MTCORE_Get_helper_rank_load_byte_counting(target_rank, is_order_required, size,
                                                  uh_win, target_h_rank_in_uh, &h_idx,
                                                  target_h_offset);
    }

    return mpi_errno;
}

#define MTCORE_Get_helper_rank(target_rank, target_seg_off, is_order_required, size, uh_win, \
        target_h_rank_in_uh, target_h_offset) \
        MTCORE_Get_helper_rank_load_opt(target_rank, target_seg_off, is_order_required, size, uh_win, \
                target_h_rank_in_uh, target_h_offset)
#else
static inline int MTCORE_Get_helper_rank_load_opt_non(int target_rank, int target_seg_off,
                                                      MTCORE_Win * uh_win,
                                                      int *target_h_rank_in_uh,
                                                      MPI_Aint * target_h_offset)
{
    int mpi_errno = MPI_SUCCESS;
    int main_h_off = uh_win->targets[target_rank].segs[target_seg_off].main_h_off;

    *target_h_rank_in_uh = uh_win->targets[target_rank].h_ranks_in_uh[main_h_off];
    *target_h_offset = uh_win->targets[target_rank].base_h_offsets[main_h_off];
    MTCORE_DBG_PRINT("[opt_non] use main helper %d, off 0x%lx for target %d seg %d\n",
                     *target_h_rank_in_uh, *target_h_offset, target_rank, target_seg_off);
    return mpi_errno;
}

#define MTCORE_Get_helper_rank(target_rank, target_seg_off, is_order_required, size, uh_win, \
        target_h_rank_in_uh, target_h_offset) \
        MTCORE_Get_helper_rank_load_opt_non(target_rank, target_seg_off, uh_win, target_h_rank_in_uh,   \
            target_h_offset)
#endif

extern int MTCORE_Op_segments_decode(const void *origin_addr, int origin_count,
                                     MPI_Datatype origin_datatype,
                                     int target_rank, MPI_Aint target_disp,
                                     int target_count, MPI_Datatype target_datatype,
                                     MTCORE_Win * uh_win, MTCORE_OP_Segment ** decoded_ops_ptr,
                                     int *num_segs);
extern int MTCORE_Op_segments_decode_basic_datatype(const void *origin_addr, int origin_count,
                                                    MPI_Datatype origin_datatype,
                                                    int target_rank, MPI_Aint target_disp,
                                                    int target_count, MPI_Datatype target_datatype,
                                                    MTCORE_Win * uh_win,
                                                    MTCORE_OP_Segment ** decoded_ops_ptr,
                                                    int *num_segs);
extern void MTCORE_Op_segments_destroy(MTCORE_OP_Segment ** decoded_ops_ptr);
extern int MTCORE_Fence_win_release_locks(MTCORE_Win * uh_win);

typedef enum {
    MTCORE_RM_COMM_TIME,
    MTCORE_RM_MAX_TYPE,
} MTCORE_Rm_type;

#ifdef MTCORE_ENABLE_RM
typedef struct {
    unsigned long long cnt;
    double time;
    double timer_sta;
    double interval_sta;
} MTCORE_Rm;

/* local runtime monitor */
extern MTCORE_Rm MTCORE_RM[MTCORE_RM_MAX_TYPE];
static inline void MTCORE_RM_COUNT(MTCORE_Rm_type type)
{
    MTCORE_RM[type].cnt++;
}

static inline void MTCORE_RM_TIMER_STR(MTCORE_Rm_type type)
{
    MTCORE_RM[type].timer_sta = PMPI_Wtime();
}

static inline void MTCORE_RM_TIMER_END(MTCORE_Rm_type type)
{
    MTCORE_RM[type].time += PMPI_Wtime() - MTCORE_RM[type].timer_sta;
}

static inline void MTCORE_RM_RESET(MTCORE_Rm_type type)
{
    MTCORE_RM[type].cnt = 0;
    MTCORE_RM[type].time = 0;
    MTCORE_RM[type].interval_sta = PMPI_Wtime();
}

static inline void MTCORE_RM_RESET_ALL()
{
    memset(MTCORE_RM, 0, sizeof(MTCORE_RM));
}
#else
#define MTCORE_RM_COUNT(type) {/*do nothing */}
#define MTCORE_RM_TIMER_STR(type) {/*do nothing */}
#define MTCORE_RM_TIMER_END(type) {/*do nothing */}
#define MTCORE_RM_RESET(type) {/*do nothing */}
#define MTCORE_RM_RESET_ALL() {/*do nothing */}
#endif

/* asynchronous state scheduling */
extern MTCORE_Async_stat MTCORE_MY_ASYNC_STAT;
/* percentage of communication in a period of execution time */
#define MTCORE_SCHED_ASYNC_THRESHOLD_DEFAULT_FREQ 50

static inline MTCORE_Async_stat MTCORE_Sched_my_async_stat()
{
    double interval;
    unsigned long long freq = 0;

#ifdef MTCORE_ENABLE_RM
    MTCORE_Async_stat old_state = MTCORE_MY_ASYNC_STAT;

    /* schedule state by using dynamic frequency */
    interval = PMPI_Wtime() - MTCORE_RM[MTCORE_RM_COMM_TIME].interval_sta;
    freq = MTCORE_RM[MTCORE_RM_COMM_TIME].time / interval * 100;

    if (freq >= MTCORE_ENV.auto_async_sched_thr_h) {
        MTCORE_MY_ASYNC_STAT = MTCORE_ASYNC_STAT_OFF;
    }
    else if (freq <= MTCORE_ENV.auto_async_sched_thr_l) {
        MTCORE_MY_ASYNC_STAT = MTCORE_ASYNC_STAT_ON;
    }

    if (old_state != MTCORE_MY_ASYNC_STAT) {
        MTCORE_WARN_PRINT("Sched async state: freq =%d\%, %s->%s \n",
                          freq, (old_state == MTCORE_ASYNC_STAT_ON) ? "on" : "off",
                          (MTCORE_MY_ASYNC_STAT == MTCORE_ASYNC_STAT_ON) ? "on" : "off");
    }

    MTCORE_RM_RESET(MTCORE_RM_COMM_TIME);
#endif
    return MTCORE_MY_ASYNC_STAT;
}

#endif /* MTCORE_H_ */
