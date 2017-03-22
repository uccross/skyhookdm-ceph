#include <iostream>
#include <thread>
#include <atomic>
#include <boost/program_options.hpp>
#include "include/rados/librados.hpp"
#include "cls/tabular/cls_tabular.h"
#include "re2/re2.h"

namespace po = boost::program_options;

static inline uint64_t __getns(clockid_t clock)
{
  struct timespec ts;
  int ret = clock_gettime(clock, &ts);
  assert(ret == 0);
  return (((uint64_t)ts.tv_sec) * 1000000000ULL) + ts.tv_nsec;
}

static inline uint64_t getns()
{
  return __getns(CLOCK_MONOTONIC);
}

#define checkret(r,v) do { \
  if (r != v) { \
    fprintf(stderr, "error %d/%s\n", r, strerror(-r)); \
    assert(0); \
    exit(1); \
  } } while (0)

static std::string string_ncopy(const char* buffer, std::size_t buffer_size) {
  const char* copyupto = std::find(buffer, buffer + buffer_size, 0);
  return std::string(buffer, copyupto);
}

static std::vector<std::string> target_objects;
static std::mutex work_lock;
static std::mutex print_lock;

static bool quiet;
static bool use_cls;
static std::string query;
static bool use_index;
static bool projection;
static uint32_t build_index_batch_size;

// query parameters
static double extended_price;
static int order_key;
static int line_number;
static int ship_date_low;
static int ship_date_high;
static double discount_low;
static double discount_high;
static double quantity;
static std::string comment_regex;

static std::atomic<unsigned> result_count;
static std::atomic<unsigned> rows_returned;

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

/*
 *
 */
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

static void worker_build_index(librados::IoCtx *ioctx)
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

static void worker(librados::IoCtx *ioctx)
{
  while (true) {
    // step 0: get object to process, or exit
    work_lock.lock();
    if (target_objects.empty()) {
      work_lock.unlock();
      break;
    }
    std::string oid = target_objects.back();
    target_objects.pop_back();
    work_lock.unlock();

    // step 1: read the object
    ceph::bufferlist bl;
    if (use_cls) {
      // populate query op
      query_op op;
      op.query = query;
      op.extended_price = extended_price;
      op.order_key = order_key;
      op.line_number = line_number;
      op.ship_date_low = ship_date_low;
      op.ship_date_high = ship_date_high;
      op.discount_low = discount_low;
      op.discount_high = discount_high;
      op.quantity = quantity;
      op.comment_regex = comment_regex;
      op.use_index = use_index;
      op.projection = projection;
      ceph::bufferlist inbl;
      ::encode(op, inbl);

      // execute it remotely
      ceph::bufferlist outbl;
      int ret = ioctx->exec(oid, "tabular", "query_op", inbl, outbl);
      checkret(ret, 0);

      // resulting rows
      ceph::bufferlist::iterator it = outbl.begin();
      try {
        ::decode(bl, it);
      } catch (ceph::buffer::error&) {
        assert(0);
      }
    } else {
      int ret = ioctx->read(oid, bl, 0, 0);
      assert(ret > 0);
      assert(bl.length() > 0);
    }

    // step 2: apply the query
    size_t row_size;
    if (projection && use_cls)
      row_size = 8;
    else
      row_size = 141;
    const char *rows = bl.c_str();
    const size_t num_rows = bl.length() / row_size;
    rows_returned += num_rows;

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
        for (size_t rid = 0; rid < num_rows; rid++) {
          const char *row = rows + rid * row_size;
          const char *cptr = row + comment_field_offset;
          const std::string comment_val = string_ncopy(cptr,
              comment_field_length);
          if (RE2::PartialMatch(comment_val, comment_regex)) {
            print_row(row);
            result_count++;
          }
        }
      }
    } else {
      assert(0);
    }
  }

  ioctx->close();
  delete ioctx;
}

int main(int argc, char **argv)
{
  std::string pool;
  unsigned num_objs;
  int nthreads;
  bool build_index;
  uint64_t test_par;
  bool test_par_read;

  po::options_description gen_opts("General options");
  gen_opts.add_options()
    ("help,h", "show help message")
		("pool", po::value<std::string>(&pool)->required(), "pool")
    ("num-objs", po::value<unsigned>(&num_objs)->required(), "num objects")
    ("use-cls", po::bool_switch(&use_cls)->default_value(false), "use cls")
    ("quiet,q", po::bool_switch(&quiet)->default_value(false), "quiet")
    ("query", po::value<std::string>(&query)->required(), "query name")
    ("nthreads", po::value<int>(&nthreads)->default_value(1), "num threads")
    ("build-index", po::bool_switch(&build_index)->default_value(false), "build index")
    ("use-index", po::bool_switch(&use_index)->default_value(false), "use index")
    ("projection", po::bool_switch(&projection)->default_value(false), "projection")
    ("test-par", po::value<uint64_t>(&test_par)->default_value(0), "test par")
    ("test-par-read", po::bool_switch(&test_par_read)->default_value(false), "test par read")
    ("build-index-batch-size", po::value<uint32_t>(&build_index_batch_size)->default_value(1000), "build index batch size")
    // query parameters
    ("extended-price", po::value<double>(&extended_price)->default_value(0.0), "extended price")
    ("order-key", po::value<int>(&order_key)->default_value(0.0), "order key")
    ("line-number", po::value<int>(&line_number)->default_value(0.0), "line number")
    ("ship-date-low", po::value<int>(&ship_date_low)->default_value(-9999), "ship date low")
    ("ship-date-high", po::value<int>(&ship_date_high)->default_value(-9999), "ship date high")
    ("discount-low", po::value<double>(&discount_low)->default_value(-9999.0), "discount low")
    ("discount-high", po::value<double>(&discount_high)->default_value(-9999.0), "discount high")
    ("quantity", po::value<double>(&quantity)->default_value(0.0), "quantity")
    ("comment_regex", po::value<std::string>(&comment_regex)->default_value(""), "comment_regex")
  ;

  po::options_description all_opts("Allowed options");
  all_opts.add(gen_opts);

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, all_opts), vm);

  if (vm.count("help")) {
    std::cout << all_opts << std::endl;
    return 1;
  }

  po::notify(vm);

  assert(num_objs > 0);
  assert(nthreads > 0);

  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  checkret(ret, 0);

  // generate the names of the objects to process
  for (unsigned oidx = 0; oidx < num_objs; oidx++) {
    std::stringstream oid_ss;
    oid_ss << "obj." << oidx;
    const std::string oid = oid_ss.str();
    target_objects.push_back(oid);
  }

  if (test_par) {
    std::vector<std::thread> threads;
    for (int i = 0; i < nthreads; i++) {
      auto ioctx = new librados::IoCtx;
      int ret = cluster.ioctx_create(pool.c_str(), *ioctx);
      checkret(ret, 0);
      threads.push_back(std::thread(worker_test_par,
            ioctx, i % num_objs, test_par, test_par_read));
    }

    for (auto& thread : threads) {
      thread.join();
    }

    return 0;
  }

  // build index for query "d"
  if (build_index) {
    std::vector<std::thread> threads;
    for (int i = 0; i < nthreads; i++) {
      auto ioctx = new librados::IoCtx;
      int ret = cluster.ioctx_create(pool.c_str(), *ioctx);
      checkret(ret, 0);
      threads.push_back(std::thread(worker_build_index, ioctx));
    }

    for (auto& thread : threads) {
      thread.join();
    }

    return 0;
  }

  /*
   * sanity check queries against provided parameters
   */
  if (query == "a") {

    assert(!use_index); // not supported
    assert(extended_price != 0.0);
    std::cout << "select count(*) from lineitem where l_extendedprice > "
      << extended_price << std::endl;

  } else if (query == "b") {

    assert(!use_index); // not supported
    assert(extended_price != 0.0);
    std::cout << "select * from lineitem where l_extendedprice > "
      << extended_price << std::endl;

  } else if (query == "c") {

    assert(!use_index); // not supported
    assert(extended_price != 0.0);
    std::cout << "select * from lineitem where l_extendedprice = "
      << extended_price << std::endl;

  } else if (query == "d") {

    if (use_index)
      assert(use_cls);

    assert(order_key != 0);
    assert(line_number != 0);
    std::cout << "select * from from lineitem where l_orderkey = "
      << order_key << " and l_linenumber = " << line_number << std::endl;

  } else if (query == "e") {

    assert(!use_index); // not supported
    assert(ship_date_low != -9999);
    assert(ship_date_high != -9999);
    assert(discount_low != -9999.0);
    assert(discount_high != -9999.0);
    assert(quantity != 0.0);
    std::cout << "select * from lineitem where l_shipdate >= "
      << ship_date_low << " and l_shipdate < " << ship_date_high
      << " and l_discount > " << discount_low << " and l_discount < "
      << discount_high << " and l_quantity < " << quantity << std::endl;

  } else if (query == "f") {

    assert(!use_index); // not supported
    assert(comment_regex != "");
    std::cout << "select * from lineitem where l_comment ilike '%"
      << comment_regex << "%'" << std::endl;

  } else {
    std::cerr << "invalid query: " << query << std::endl;
    exit(1);
  }

  result_count = 0;
  rows_returned = 0;

  // start worker threads
  std::vector<std::thread> threads;
  for (int i = 0; i < nthreads; i++) {
    auto ioctx = new librados::IoCtx;
    int ret = cluster.ioctx_create(pool.c_str(), *ioctx);
    checkret(ret, 0);
    threads.push_back(std::thread(worker, ioctx));
  }

  // the threads will exit when all the objects are processed
  for (auto& thread : threads) {
    thread.join();
  }

  if (query == "a" && use_cls) {
    std::cout << "total result row count: " << result_count
      << " / -1" << std::endl;
  } else {
    std::cout << "total result row count: " << result_count
      << " / " << rows_returned << std::endl;
  }

  return 0;
}
