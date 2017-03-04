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

int main(int argc, char **argv)
{
  std::string pool;
  unsigned num_objs;
  size_t row_size;
  size_t field_offset;
  size_t field_length;
  std::string regex;
  bool use_cls;

  po::options_description gen_opts("General options");
  gen_opts.add_options()
    ("help,h", "show help message")
		("pool", po::value<std::string>(&pool)->required(), "pool")
    ("num-objs", po::value<unsigned>(&num_objs)->required(), "num objects")
    ("row-size", po::value<size_t>(&row_size)->required(), "row size bytes")
    ("field-offset", po::value<size_t>(&field_offset)->required(), "field offset")
    ("field-length", po::value<size_t>(&field_length)->required(), "field length")
    ("regex", po::value<std::string>(&regex)->default_value(""), "regex")
    ("use-cls", po::bool_switch(&use_cls)->default_value(false), "use cls")
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
  assert(row_size > 0);
  assert((field_offset + field_length) <= row_size);

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

  for (unsigned o = 0; o < num_objs; o++) {
    std::stringstream ss;
    ss << "obj." << o;
    const std::string oid = ss.str();

    ceph::bufferlist bl;
    if (use_cls) {
      regex_scan_op op;
      op.row_size = row_size;
      op.field_offset = field_offset;
      op.field_length = field_length;
      op.regex = regex;
      ceph::bufferlist inbl;
      ::encode(op, inbl);
      ceph::bufferlist outbl;
      int ret = ioctx.exec(oid, "tabular", "regex_scan", inbl, outbl);
      checkret(ret, 0);
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

    size_t count = 0;
    const char *rows = bl.c_str();
    size_t num_rows = bl.length() / row_size;

    for (size_t r = 0; r < num_rows; r++) {
      const char *row = rows + r * row_size;
      const char *cptr = row + field_offset;
      const std::string comment = string_ncopy(cptr, field_length);

      if (regex != "") {
        if (RE2::PartialMatch(comment, regex)) {
          std::cout << comment << std::endl;
          count++;
        }
      } else {
          std::cout << comment << std::endl;
          count++;
      }
    }

    std::cout << oid << ": filtered " << count << " / " << num_rows << std::endl;
  }

  return 0;
}
