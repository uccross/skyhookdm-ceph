#include <iostream>
#include <boost/program_options.hpp>
#include "include/rados/librados.hpp"
#include "cls/tabular/cls_tabular.h"

namespace po = boost::program_options;

#define checkret(r,v) do { \
  if (r != v) { \
    fprintf(stderr, "error %d/%s\n", r, strerror(-r)); \
    assert(0); \
    exit(1); \
  } } while (0)

int main(int argc, char **argv)
{
  uint64_t range_size;
  unsigned num_rows;
  unsigned rows_per_obj;
  double selectivity;
  std::string pool;
  bool use_cls;
  bool use_index;
  bool use_pg;

  po::options_description gen_opts("General options");
  gen_opts.add_options()
    ("help,h", "show help message")
    ("range-size", po::value<uint64_t>(&range_size)->required(), "data range")
    ("num-rows", po::value<unsigned>(&num_rows)->required(), "number of rows")
    ("rows-per-obj", po::value<unsigned>(&rows_per_obj)->required(), "rows per object")
    ("selectivity", po::value<double>(&selectivity)->required(), "selectivity pct")
    ("use-cls", po::bool_switch(&use_cls)->default_value(false), "filter in cls")
    ("use-pg", po::bool_switch(&use_pg)->default_value(false), "filter in pg")
    ("use-index", po::bool_switch(&use_index)->default_value(false), "use cls index")
		("pool,p", po::value<std::string>(&pool)->required(), "pool")
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

  assert(range_size > 0);
  assert(num_rows > 0);
  assert(rows_per_obj > 0);
  assert(num_rows % rows_per_obj == 0);

  assert(selectivity >= 0.0);
  assert(selectivity <= 100.0);
  selectivity /= 100.0;

  assert((!use_cls && !use_pg) ||
      (use_cls && !use_pg) ||
      (!use_cls && use_pg));
  if (use_index)
    assert(use_cls || use_pg);

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

  const uint64_t max_val = range_size * selectivity;
  const unsigned num_objs = num_rows / rows_per_obj;

  // build list of objects involved. this is just used for verification
  std::set<std::string> all_oids;
  for (unsigned i = 0; i < num_objs; i++) {
    std::stringstream ss;
    ss << "obj." << i;
    const std::string oid = ss.str();
    auto r = all_oids.emplace(oid);
    assert(r.second);
  }

  std::set<std::string> seen_oids;
  assert(num_objs > 0);
  assert(all_oids.size() == num_objs);
  uint64_t filtered_rows = 0;
  uint64_t objects_read = 0;
  uint64_t bytes_read = 0;

  if (use_cls || (!use_cls && !use_pg)) {
    if (use_cls)
      assert(!use_pg);

    // for each object in the data set
    for (unsigned o = 0; o < num_objs; o++) {
      std::stringstream ss;
      ss << "obj." << o;
      const std::string oid = ss.str();

      // read rows
      ceph::bufferlist bl;
      if (use_cls) {
        scan_op op;
        op.max_val = max_val;
        op.use_index = use_index;
        ceph::bufferlist inbl;
        ::encode(op, inbl);
        ceph::bufferlist outbl;
        int ret = ioctx.exec(oid, "tabular", "scan", inbl, outbl);
        checkret(ret, 0);

        ceph::bufferlist::iterator it = outbl.begin();
        try {
          bool index_skipped;
          ::decode(index_skipped, it);
          ::decode(bl, it);
          if (!index_skipped)
            objects_read++;
          else
            assert(use_index);
        } catch (ceph::buffer::error&) {
          assert(0);
        }
      } else {
        int ret = ioctx.read(oid, bl, 0, 0);
        assert(ret > 0);
        assert(bl.length() > 0);
        assert(bl.length() / sizeof(uint64_t) == rows_per_obj);
        objects_read++;
      }

      bytes_read += bl.length();

      const uint64_t row_count = bl.length() / sizeof(uint64_t);
      const uint64_t *rows = (uint64_t*)bl.c_str();

      if (use_cls) {
        for (unsigned r = 0; r < row_count; r++) {
          const uint64_t row_val = rows[r];
          assert(row_val < max_val);
          filtered_rows++;
        }
      } else {
        for (unsigned r = 0; r < row_count; r++) {
          const uint64_t row_val = rows[r];
          if (row_val < max_val)
            filtered_rows++;
        }
      }

      auto r = seen_oids.insert(oid);
      assert(r.second);
    }
  }

  if (use_pg) {
    assert(seen_oids.empty());
    assert(!use_cls);

    void *scan_context = NULL;
    ioctx.tabular_scan_alloc_context(&scan_context);
    librados::TabularScanUserContext user_context;

    user_context.use_index = use_index;
    user_context.max_val = max_val;
    user_context.max_size = 8<<20;

    while (true) {
      int ret = ioctx.tabular_scan(scan_context, &user_context);
      if (ret == -ERANGE)
        break;
      else
        checkret(ret, 0);

      const uint64_t row_count = user_context.bl.length() / sizeof(uint64_t);
      const uint64_t *rows = (uint64_t*)user_context.bl.c_str();

      bytes_read += user_context.bl.length();

      for (unsigned r = 0; r < row_count; r++) {
        const uint64_t row_val = rows[r];
        if (row_val < max_val)
          filtered_rows++;
      }

      for (auto oid : user_context.oids) {
        auto r = seen_oids.insert(oid.first);
        assert(r.second);
        if (!oid.second) // index_skipped?
          objects_read++;
      }
    }

    ioctx.tabular_scan_free_context(scan_context);
  }

  assert(all_oids == seen_oids);

  std::cout << "total rows " << num_rows
    << " filtered rows " << filtered_rows
    << "; selectivity wanted " << (100.0 * selectivity)
    << "; selectivity observed "
    << (100.0 * (double)filtered_rows / (double)num_rows)
    << "; objects read " << objects_read << "/" << num_objs
    << "; data read " << bytes_read
    << std::endl;

  ioctx.close();
  cluster.shutdown();

  return 0;
}
