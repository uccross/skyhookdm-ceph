/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/

#include <iostream>
#include <algorithm>
#include <atomic>
#include <thread>
#include <condition_variable>
#include "include/rados/librados.hpp"
#include "cls/tabular/cls_tabular.h"
#include "cls/tabular/cls_tabular_utils.h"
#include "cls/tabular/cls_tabular_processing.h"
#include "re2/re2.h"

extern inline uint64_t __getns(clockid_t clock)
{
  struct timespec ts;
  int ret = clock_gettime(clock, &ts);
  assert(ret == 0);
  return (((uint64_t)ts.tv_sec) * 1000000000ULL) + ts.tv_nsec;
}

extern inline uint64_t getns()
{
  return __getns(CLOCK_MONOTONIC);
}

#define checkret(r,v) do { \
  if (r != v) { \
    fprintf(stderr, "error %d/%s\n", r, strerror(-r)); \
    assert(0); \
    exit(1); \
  } } while (0)

struct timing {
  uint64_t dispatch;
  uint64_t response;
  uint64_t read_ns;
  uint64_t eval_ns;
  uint64_t eval2_ns;
};

struct AioState {
  ceph::bufferlist bl;
  librados::AioCompletion *c;
  timing times;
};

extern bool quiet;
extern bool use_cls;
extern std::string query;
extern bool use_index;
extern bool old_projection;
extern uint32_t index_batch_size;
extern uint64_t extra_row_cost;

extern std::vector<timing> timings;

// query parameters to be encoded into query_op struct

// query params old
extern double extended_price;
extern int order_key;
extern int line_number;
extern int ship_date_low;
extern int ship_date_high;
extern double discount_low;
extern double discount_high;
extern double quantity;
extern std::string comment_regex;

// query_op params new
extern bool debug;
extern bool qop_fastpath;
extern bool qop_index_read;
extern bool qop_mem_constrain;
extern int qop_index_type;
extern int qop_index2_type;
extern int qop_index_plan_type;
extern int qop_index_batch_size;
extern int qop_result_format;  // SkyFormatType enum
extern std::string qop_db_schema_name;
extern std::string qop_table_name;
extern std::string qop_data_schema;
extern std::string qop_query_schema;
extern std::string qop_index_schema;
extern std::string qop_index2_schema;
extern std::string qop_query_preds;
extern std::string qop_index_preds;
extern std::string qop_index2_preds;

extern bool idx_op_idx_unique;
extern bool idx_op_ignore_stopwords;
extern int idx_op_batch_size;
extern int idx_op_idx_type;
extern std::string idx_op_idx_schema;
extern std::string idx_op_text_delims;

// Transform op params
extern int trans_op_format_type;

// Example op params
extern int expl_func_counter;
extern int expl_func_id;

// HEP op params
extern std::string qop_dataset_name;
extern std::string qop_file_name;
extern std::string qop_tree_name;

// other exec flags
extern bool runstats;
extern std::string project_cols;

// for debugging, prints full record header and metadata
extern bool print_verbose;

// final output format
extern int skyhook_output_format;  // SkyFormatType enum

// to convert strings <=> skyhook data structs
extern Tables::schema_vec sky_tbl_schema;
extern Tables::schema_vec sky_qry_schema;
extern Tables::schema_vec sky_idx_schema;
extern Tables::schema_vec sky_idx2_schema;
extern Tables::predicate_vec sky_qry_preds;
extern Tables::predicate_vec sky_idx_preds;
extern Tables::predicate_vec sky_idx2_preds;

extern std::atomic<unsigned> result_count;
extern std::atomic<unsigned> rows_returned;

// used for print csv
extern std::atomic<bool> print_header;
extern std::atomic<long long int> row_counter;
extern long long int row_limit;

// rename work_lock
extern int outstanding_ios;
extern std::vector<std::string> target_objects;
extern std::list<AioState*> ready_ios;

extern std::mutex dispatch_lock;
extern std::condition_variable dispatch_cond;

extern std::mutex work_lock;
extern std::condition_variable work_cond;

extern bool stop;

// worker tasks for threads, corresponding to our cls methods
void worker_build_index(librados::IoCtx *ioctx);
void worker_exec_build_sky_index_op(librados::IoCtx *ioctx, idx_op op);
void worker_exec_runstats_op(librados::IoCtx *ioctx, stats_op op);
void worker_transform_db_op(librados::IoCtx *ioctx, transform_op op);
void worker_exec_query_op();  // default worker task for exec_query_op
void handle_cb(librados::completion_t cb, void *arg);
void worker_lock_obj_init_op(librados::IoCtx *ioctx, lockobj_info op);
void worker_lock_obj_free_op(librados::IoCtx *ioctx, lockobj_info op);
void worker_lock_obj_get_op(librados::IoCtx *ioctx, lockobj_info op);
void worker_lock_obj_acquire_op(librados::IoCtx *ioctx, lockobj_info op);
void worker_lock_obj_create_op(librados::IoCtx *ioctx, lockobj_info op);
