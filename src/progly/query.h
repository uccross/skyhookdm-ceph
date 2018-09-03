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
extern std::string table_schema_str;
extern std::string query_schema_str;
extern bool fastpath;

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
void worker();
void handle_cb(librados::completion_t cb, void *arg);
