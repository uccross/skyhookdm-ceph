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
extern bool projection;
extern uint32_t build_index_batch_size;
extern uint64_t extra_row_cost;

extern std::vector<timing> timings;

// query parameters to be encoded into query_op struct
extern double extended_price;
extern int order_key;
extern int line_number;
extern int ship_date_low;
extern int ship_date_high;
extern double discount_low;
extern double discount_high;
extern double quantity;
extern std::string comment_regex;

// query_op params for flatbufs
extern bool qop_fastpath;
extern bool qop_index_read;
extern bool qop_index_create;
extern int qop_index_type;
extern int qop_index2_type;
extern int qop_index_plan_type;
extern std::string qop_db_schema;
extern std::string qop_table_name;
extern std::string qop_data_schema;
extern std::string qop_query_schema;
extern std::string qop_index_schema;
extern std::string qop_index2_schema;
extern std::string qop_query_preds;
extern std::string qop_index_preds;
extern std::string qop_index2_preds;

// build index op params for flatbufs
extern bool idx_op_idx_unique;
extern bool idx_op_ignore_stopwords;
extern int idx_op_batch_size;
extern int idx_op_idx_type;
extern std::string idx_op_idx_schema;
extern std::string idx_op_delims;

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

// total number of rows processed, client side or server side (cls).
extern std::atomic<unsigned> nrows_processed;

// rename work_lock
extern int outstanding_ios;
extern std::vector<std::string> target_objects;
extern std::list<AioState*> ready_ios;

extern std::mutex dispatch_lock;
extern std::condition_variable dispatch_cond;

extern std::mutex work_lock;
extern std::condition_variable work_cond;

extern bool stop;

void worker_build_index(librados::IoCtx *ioctx);
void worker_build_sky_index(librados::IoCtx *ioctx, idx_op op);
void worker();
void handle_cb(librados::completion_t cb, void *arg);
