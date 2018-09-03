/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#include <fstream>
#include "query.h"

static std::string string_ncopy(const char* buffer, std::size_t buffer_size) {
  const char* copyupto = std::find(buffer, buffer + buffer_size, 0);
  return std::string(buffer, copyupto);
}

static std::mutex print_lock;

bool quiet;
bool use_cls;
std::string query;
bool use_index;
bool projection;
uint32_t build_index_batch_size;
uint64_t extra_row_cost;

std::vector<timing> timings;

// query parameters to be encoded into query_op struct
double extended_price;
int order_key;
int line_number;
int ship_date_low;
int ship_date_high;
double discount_low;
double discount_high;
double quantity;
std::string comment_regex;
std::string table_schema_str;
std::string query_schema_str;
bool fastpath;

std::atomic<unsigned> result_count;
std::atomic<unsigned> rows_returned;

// total number of rows processed, client side or server side (cls).
std::atomic<unsigned> nrows_processed;

// rename work_lock
int outstanding_ios;
std::vector<std::string> target_objects;
std::list<AioState*> ready_ios;

std::mutex dispatch_lock;
std::condition_variable dispatch_cond;

std::mutex work_lock;
std::condition_variable work_cond;

bool stop;

static void print_row(const char *row)
{
  if (quiet)
    return;

  print_lock.lock();

  const size_t order_key_field_offset = 0;
  size_t line_number_field_offset;
  if (projection && use_cls)
    line_number_field_offset = 4;
  else
    line_number_field_offset = 12;
  const size_t quantity_field_offset = 16;
  const size_t extended_price_field_offset = 24;
  const size_t discount_field_offset = 32;
  const size_t shipdate_field_offset = 50;
  const size_t comment_field_offset = 97;
  const size_t comment_field_length = 44;

  const double extended_price = *((const double *)(row + extended_price_field_offset));
  const int order_key = *((const int *)(row + order_key_field_offset));
  const int line_number = *((const int *)(row + line_number_field_offset));
  const int ship_date = *((const int *)(row + shipdate_field_offset));
  const double discount = *((const double *)(row + discount_field_offset));
  const double quantity = *((const double *)(row + quantity_field_offset));
  const std::string comment = string_ncopy(row + comment_field_offset,
      comment_field_length);

  if (projection) {
    std::cout << order_key <<
      "|" << line_number <<
      std::endl;
  } else {
    std::cout << extended_price <<
      "|" << order_key <<
      "|" << line_number <<
      "|" << ship_date <<
      "|" << discount <<
      "|" << quantity <<
      "|" << comment <<
      std::endl;
  }

  print_lock.unlock();
}

static void print_fb(const char *fb, size_t fb_size, vector<Tables::col_info> &schema)
{
    if (quiet)
        return;

    print_lock.lock();
    Tables::printSkyFb(fb, fb_size, schema);
    print_lock.unlock();
}

static const size_t order_key_field_offset = 0;
static const size_t line_number_field_offset = 12;
static const size_t quantity_field_offset = 16;
static const size_t extended_price_field_offset = 24;
static const size_t discount_field_offset = 32;
static const size_t shipdate_field_offset = 50;
static const size_t comment_field_offset = 97;
static const size_t comment_field_length = 44;

static void worker_test_par(librados::IoCtx *ioctx, int i, uint64_t iters,
    bool test_par_read)
{
  std::stringstream ss;
  ss << "obj." << i;
  const std::string oid = ss.str();

  int ret = ioctx->create(oid, false);
  checkret(ret, 0);

  while (true) {
    ceph::bufferlist inbl, outbl;
    ::encode(iters, inbl);
    ::encode(test_par_read, inbl);
    ret = ioctx->exec(oid, "tabular", "test_par", inbl, outbl);
    checkret(ret, 0);
  }
}

void worker_build_index(librados::IoCtx *ioctx)
{
  while (true) {
    work_lock.lock();
    if (target_objects.empty()) {
      work_lock.unlock();
      break;
    }
    std::string oid = target_objects.back();
    target_objects.pop_back();
    std::cout << "building index... " << oid << std::endl;
    work_lock.unlock();

    ceph::bufferlist inbl, outbl;
    ::encode(build_index_batch_size, inbl);
    int ret = ioctx->exec(oid, "tabular", "build_index", inbl, outbl);
    checkret(ret, 0);
  }
  ioctx->close();
}

// busy loop work to simulate high cpu cost ops
volatile uint64_t __tabular_x;
static void add_extra_row_cost(uint64_t cost)
{
  for (uint64_t i = 0; i < cost; i++) {
    __tabular_x += i;
  }
}

void worker()
{
  std::unique_lock<std::mutex> lock(work_lock);
  while (true) {
    // wait for work, or done
    if (ready_ios.empty()) {
      if (stop)
        break;
      work_cond.wait(lock);
      continue;
    }

    // prepare result
    AioState *s = ready_ios.front();
    ready_ios.pop_front();

    // process result without lock. we own it now.
    lock.unlock();

    dispatch_lock.lock();
    outstanding_ios--;
    dispatch_lock.unlock();
    dispatch_cond.notify_one();

    struct timing times = s->times;
    uint64_t nrows_server_processed = 0;
    uint64_t eval2_start = getns();

    if (query == "flatbuf") {

        // librados read will return the original obj, which is a seq of bls.
        // cls read will return an obj with some stats and a seq of bls.

        bufferlist wrapped_bls;   // to store the seq of bls.

        // first extract the top-level statistics encoded during cls processing
        if (use_cls) {
            try {
                ceph::bufferlist::iterator it = s->bl.begin();
                ::decode(times.read_ns, it);
                ::decode(times.eval_ns, it);
                ::decode(nrows_server_processed, it);
                ::decode(wrapped_bls, it);  // contains a seq of encoded bls.
            } catch (ceph::buffer::error&) {
                int decode_runquery_cls = 0;
                assert(decode_runquery_cls);
            }
            nrows_processed += nrows_server_processed;
        } else {
            wrapped_bls = s->bl;  // contains a seq of encoded bls.
        }
        delete s;  // we're done processing all of the bls contained within

        // decode and process each bl (contains 1 flatbuf) in a loop.
        ceph::bufferlist::iterator it = wrapped_bls.begin();
        while (it.get_remaining() > 0) {
            ceph::bufferlist bl;
            try {
                ::decode(bl, it);  // unpack the next bl (flatbuf)
            } catch (ceph::buffer::error&) {
                int decode_runquery_noncls = 0;
                assert(decode_runquery_noncls);
            }

            // get our data as contiguous bytes before accessing as flatbuf
            const char* fb = bl.c_str();
            size_t fb_size = bl.length();
            Tables::sky_root_header root = \
                    Tables::getSkyRootHeader(fb, fb_size);

            // local counter to accumulate nrows in all flatbuffers received.
            rows_returned += root.nrows;

            int ret = 0;
            if (use_cls) {
                /* Server side processing already done at this point.
                 * TODO: any further client-side processing required here,
                 * such as possibly aggregate global ops here (e.g.,count/sort)
                 * then convert rows to valid db tuple format.
                 */

                // server has already applied its predicates, so count all rows
                // here that pass after applying the remaining global ops.
                result_count += root.nrows;

                Tables::schema_vec schema_out;
                ret = getSchemaFromSchemaString(schema_out, query_schema_str);
                assert(ret!=Tables::TablesErrCodes::EmptySchema);
                assert(ret!=Tables::TablesErrCodes::BadColInfoFormat);
                print_fb(fb, fb_size, schema_out);
            } else {
                // perform any extra project/select/agg if needed.
                bool more_processing = false;
                const char *fb_out;
                size_t fb_out_size;
                Tables::schema_vec schema_out;
                nrows_processed += root.nrows;

                // set the input schema (current schema of the fb)
                Tables::schema_vec schema_in;
                ret = getSchemaFromSchemaString(schema_in, table_schema_str);
                assert(ret!=Tables::TablesErrCodes::EmptySchema); // TODO errs
                assert(ret!=Tables::TablesErrCodes::BadColInfoFormat);
                ret = getSchemaFromSchemaString(schema_out, query_schema_str);
                assert(ret!=Tables::TablesErrCodes::EmptySchema);
                assert(ret!=Tables::TablesErrCodes::BadColInfoFormat);

                // set the output schema
                if (projection) {
                    more_processing = true;
                }

                if (!more_processing) {  // nothing left to do here.
                    fb_out = fb;
                    fb_out_size = fb_size;
                    result_count += root.nrows;
                } else {
                    flatbuffers::FlatBufferBuilder flatbldr(1024); // pre-alloc
                    std::string errmsg;
                    bool processedOK = true;
                    ret = processSkyFb(flatbldr, schema_in, schema_out,
                                       fb, fb_size, errmsg);
                    if (ret != 0) {
                        processedOK = false;
                        std::cerr << "ERROR: run-query() processing flatbuf: "
                                  << errmsg << endl;
                        std::cerr << "ERROR: Tables::ErrCodes=" << ret << endl;
                        assert(processedOK);
                    }
                    fb_out = reinterpret_cast<char*>(
                            flatbldr.GetBufferPointer());
                    fb_out_size = flatbldr.GetSize();
                    Tables::sky_root_header skyroot = \
                            Tables::getSkyRootHeader(fb_out, fb_out_size);
                    result_count += skyroot.nrows;
                }
                print_fb(fb_out, fb_out_size, schema_out);
            }
        } // endloop of processing sequence of encoded bls

    } else {   // older processing code below

        ceph::bufferlist bl;
        // if it was a cls read, first unpack some of the cls processing stats
        if (use_cls) {
            try {
                ceph::bufferlist::iterator it = s->bl.begin();
                ::decode(times.read_ns, it);
                ::decode(times.eval_ns, it);
                ::decode(nrows_server_processed, it);
                ::decode(bl, it);
            } catch (ceph::buffer::error&) {
                int decode_runquery_cls = 0;
                assert(decode_runquery_cls);
            }
        } else {
            bl = s->bl;
        }

        // data is now all in bl
        delete s;

        // our older query processing code below...
        // apply the query
        size_t row_size;
        if (projection && use_cls)
          row_size = 8;
        else
          row_size = 141;
        const char *rows = bl.c_str();
        const size_t num_rows = bl.length() / row_size;
        rows_returned += num_rows;

        if (use_cls)
            nrows_processed += nrows_server_processed;
        else
            nrows_processed += num_rows;

        if (query == "a") {
          if (use_cls) {
            // if we are using cls then storage system returns the number of
            // matching rows rather than the actual rows. so we patch up the
            // results to the presentation of the results is correct.
            size_t matching_rows;
            ceph::bufferlist::iterator it = bl.begin();
            ::decode(matching_rows, it);
            result_count += matching_rows;
          } else {
            if (projection && use_cls) {
              result_count += num_rows;
            } else {
              for (size_t rid = 0; rid < num_rows; rid++) {
                const char *row = rows + rid * row_size;
                const char *vptr = row + extended_price_field_offset;
                const double val = *(const double*)vptr;
                if (val > extended_price) {
                  result_count++;
                  // when a predicate passes, add some extra work
                  add_extra_row_cost(extra_row_cost);
                }
              }
            }
          }
        } else if (query == "b") {
          if (projection && use_cls) {
            for (size_t rid = 0; rid < num_rows; rid++) {
              const char *row = rows + rid * row_size;
              print_row(row);
              result_count++;
            }
          } else {
            for (size_t rid = 0; rid < num_rows; rid++) {
              const char *row = rows + rid * row_size;
              const char *vptr = row + extended_price_field_offset;
              const double val = *(const double*)vptr;
              if (val > extended_price) {
                print_row(row);
                result_count++;
                add_extra_row_cost(extra_row_cost);
              }
            }
          }
        } else if (query == "c") {
          if (projection && use_cls) {
            for (size_t rid = 0; rid < num_rows; rid++) {
              const char *row = rows + rid * row_size;
              print_row(row);
              result_count++;
            }
          } else {
            for (size_t rid = 0; rid < num_rows; rid++) {
              const char *row = rows + rid * row_size;
              const char *vptr = row + extended_price_field_offset;
              const double val = *(const double*)vptr;
              if (val == extended_price) {
                print_row(row);
                result_count++;
                add_extra_row_cost(extra_row_cost);
              }
            }
          }
        } else if (query == "d") {
          if (projection && use_cls) {
            for (size_t rid = 0; rid < num_rows; rid++) {
              const char *row = rows + rid * row_size;
              print_row(row);
              result_count++;
            }
          } else {
            for (size_t rid = 0; rid < num_rows; rid++) {
              const char *row = rows + rid * row_size;
              const char *vptr = row + order_key_field_offset;
              const int order_key_val = *(const int*)vptr;
              if (order_key_val == order_key) {
                const char *vptr = row + line_number_field_offset;
                const int line_number_val = *(const int*)vptr;
                if (line_number_val == line_number) {
                  print_row(row);
                  result_count++;
                  add_extra_row_cost(extra_row_cost);
                }
              }
            }
          }
        } else if (query == "e") {
          if (projection && use_cls) {
            for (size_t rid = 0; rid < num_rows; rid++) {
              const char *row = rows + rid * row_size;
              print_row(row);
              result_count++;
            }
          } else {
            for (size_t rid = 0; rid < num_rows; rid++) {
              const char *row = rows + rid * row_size;

              const int shipdate_val = *((const int *)(row + shipdate_field_offset));
              if (shipdate_val >= ship_date_low && shipdate_val < ship_date_high) {
                const double discount_val = *((const double *)(row + discount_field_offset));
                if (discount_val > discount_low && discount_val < discount_high) {
                  const double quantity_val = *((const double *)(row + quantity_field_offset));
                  if (quantity_val < quantity) {
                    print_row(row);
                    result_count++;
                    add_extra_row_cost(extra_row_cost);
                  }
                }
              }
            }
          }
        } else if (query == "f") {
          if (projection && use_cls) {
            for (size_t rid = 0; rid < num_rows; rid++) {
              const char *row = rows + rid * row_size;
              print_row(row);
              result_count++;
            }
          } else {
            RE2 re(comment_regex);
            for (size_t rid = 0; rid < num_rows; rid++) {
              const char *row = rows + rid * row_size;
              const char *cptr = row + comment_field_offset;
              const std::string comment_val = string_ncopy(cptr,
                  comment_field_length);
              if (RE2::PartialMatch(comment_val, re)) {
                print_row(row);
                result_count++;
                add_extra_row_cost(extra_row_cost);
              }
            }
          }
        } else if (query == "fastpath") {
            for (size_t rid = 0; rid < num_rows; rid++) {
              const char *row = rows + rid * row_size;
              print_row(row);
              result_count++;
            }
        } else {
          assert(0);
        }
    }

    times.eval2_ns = getns() - eval2_start;

    lock.lock();
    timings.push_back(times);
  }
}

/*
 * 1. free up aio resources
 * 2. put io on work queue
 * 3. wake-up a worker
 */
void handle_cb(librados::completion_t cb, void *arg)
{
  AioState *s = (AioState*)arg;
  s->times.response = getns();
  assert(s->c->get_return_value() >= 0);
  s->c->release();
  s->c = NULL;

  work_lock.lock();
  ready_ios.push_back(s);
  work_lock.unlock();

  work_cond.notify_one();
}
