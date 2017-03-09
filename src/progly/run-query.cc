#include <iostream>
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

std::string string_ncopy(const char* buffer, std::size_t buffer_size) {
  const char* copyupto = std::find(buffer, buffer + buffer_size, 0);
  return std::string(buffer, copyupto);
}

static bool quiet;

static void print_row(const char *row)
{
  if (quiet)
    return;

  const size_t order_key_field_offset = 0;
  const size_t line_number_field_offset = 12;
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

  std::cout << extended_price <<
    "|" << order_key <<
    "|" << line_number <<
    "|" << ship_date <<
    "|" << discount <<
    "|" << quantity <<
    "|" << comment <<
    std::endl;
}

int main(int argc, char **argv)
{
  std::string pool;
  unsigned num_objs;
  std::string query;
  bool use_cls;

  // query parameters
  double extended_price;
  int order_key;
  int line_number;
  int ship_date_low;
  int ship_date_high;
  double discount_low;
  double discount_high;
  double quantity;
  std::string comment_regex;
  //std::vector<std::string> projection;

  po::options_description gen_opts("General options");
  gen_opts.add_options()
    ("help,h", "show help message")
		("pool", po::value<std::string>(&pool)->required(), "pool")
    ("num-objs", po::value<unsigned>(&num_objs)->required(), "num objects")
    ("use-cls", po::bool_switch(&use_cls)->default_value(false), "use cls")
    ("quiet,q", po::bool_switch(&quiet)->default_value(false), "quiet")
    ("query", po::value<std::string>(&query)->required(), "query name")
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
    //("projection", po::value<std::vector<std::string>>(&projection)->multitoken(), "projection")
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

  /*
   * sanity check queries against provided parameters
   */
  if (query == "a") {

    assert(extended_price != 0.0);
    std::cout << "select count(*) from lineitem where l_extendedprice > "
      << extended_price << std::endl;

  } else if (query == "b") {

    assert(extended_price != 0.0);
    std::cout << "select * from lineitem where l_extendedprice > "
      << extended_price << std::endl;

  } else if (query == "c") {

    assert(extended_price != 0.0);
    std::cout << "select * from lineitem where l_extendedprice = "
      << extended_price << std::endl;

  } else if (query == "d") {

    assert(order_key != 0.0);
    assert(line_number != 0.0);
    std::cout << "select * from from lineitem where l_orderkey = "
      << order_key << " and l_linenumber = " << line_number << std::endl;

  } else if (query == "e") {

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

    assert(comment_regex != "");
    std::cout << "select * from lineitem where l_comment ilike '%"
      << comment_regex << "%'" << std::endl;

  } else {
    std::cerr << "invalid query: " << query << std::endl;
    exit(1);
  }

  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  checkret(ret, 0);

  // open pool i/o context
  librados::IoCtx ioctx;
  ret = cluster.ioctx_create(pool.c_str(), ioctx);
  checkret(ret, 0);

  /*
   *
   */
  const size_t order_key_field_offset = 0;
  const size_t line_number_field_offset = 12;
  const size_t quantity_field_offset = 16;
  const size_t extended_price_field_offset = 24;
  const size_t discount_field_offset = 32;
  const size_t shipdate_field_offset = 50;
  const size_t comment_field_offset = 97;
  const size_t comment_field_length = 44;

  // for each object we read the object from rados and then apply the query
  // to the rows that are returned. when --use-cls is specified the query is
  // also applied in the osd before returning the results.
  size_t result_count = 0;
  size_t rows_returned = 0;

  for (unsigned oidx = 0; oidx < num_objs; oidx++) {

    // name of object is: obj.oidx
    std::stringstream oid_ss;
    oid_ss << "obj." << oidx;
    const std::string oid = oid_ss.str();

    // step 1: read the object
    ceph::bufferlist bl;
    if (use_cls) {
      // populate query op
      query_op op;
      op.query = query;
      op.extended_price = extended_price;
      op.ship_date_low = ship_date_low;
      op.ship_date_high = ship_date_high;
      op.discount_low = discount_low;
      op.discount_high = discount_high;
      op.quantity = quantity;
      op.comment_regex = comment_regex;
      ceph::bufferlist inbl;
      ::encode(op, inbl);

      // execute it remotely
      ceph::bufferlist outbl;
      int ret = ioctx.exec(oid, "tabular", "query_op", inbl, outbl);
      checkret(ret, 0);

      // resulting rows
      ceph::bufferlist::iterator it = outbl.begin();
      try {
        ::decode(bl, it);
      } catch (ceph::buffer::error&) {
        assert(0);
      }
    } else {
      int ret = ioctx.read(oid, bl, 0, 0);
      assert(ret > 0);
      assert(bl.length() > 0);
    }

    // step 2: apply the query
    const size_t row_size = 141;
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
        for (size_t rid = 0; rid < num_rows; rid++) {
          const char *row = rows + rid * row_size;
          const char *vptr = row + extended_price_field_offset;
          const double val = *(const double*)vptr;
          if (val > extended_price) {
            result_count++;
          }
        }
      }
    } else if (query == "b") {
      for (size_t rid = 0; rid < num_rows; rid++) {
        const char *row = rows + rid * row_size;
        const char *vptr = row + extended_price_field_offset;
        const double val = *(const double*)vptr;
        if (val > extended_price) {
          print_row(row);
          result_count++;
        }
      }
    } else if (query == "c") {
      for (size_t rid = 0; rid < num_rows; rid++) {
        const char *row = rows + rid * row_size;
        const char *vptr = row + extended_price_field_offset;
        const double val = *(const double*)vptr;
        if (val == extended_price) {
          print_row(row);
          result_count++;
        }
      }
    } else if (query == "d") {
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
    } else if (query == "e") {
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
    } else if (query == "f") {
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
    } else {
      assert(0);
    }

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
