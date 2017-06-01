#include <iostream>
#include <thread>
#include <atomic>
#include <fstream>
#include <algorithm>
#include <condition_variable>
#include <boost/program_options.hpp>
#include "include/rados/librados.hpp"

namespace po = boost::program_options;

static inline uint64_t __getns(clockid_t clock)
{
  struct timespec ts;
  int ret = clock_gettime(clock, &ts);
  assert(ret == 0);
  return (((uint64_t)ts.tv_sec) * 1000000000ULL) + ts.tv_nsec;
}

static inline uint64_t getns_rt()
{
  return __getns(CLOCK_REALTIME);
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

static std::mutex lock;
static std::list<std::string> src_oids;
static size_t row_size;
static size_t obj_size;
static size_t rows_per_obj;
static unsigned long run_id;

struct opinfo {
  uint64_t read_start;
  uint64_t write_start;
  uint64_t write_end;
};

static std::list<opinfo> results;

static void worker_split(librados::IoCtx *ioctx)
{
  std::list<opinfo> ops;

  std::string oid;
  while (true) {
    {
      std::lock_guard<std::mutex> l(lock);
      if (src_oids.empty()) {
				auto it = results.end();
        results.splice(it, ops);
        return;
      }
      oid = src_oids.front();
      src_oids.pop_front();
    }

    struct opinfo op;

    // read source object
    ceph::bufferlist src;
    src.reserve(obj_size);
    op.read_start = getns_rt();
    int ret = ioctx->read(oid, src, 0, 0);
    assert(ret == (int)obj_size);
    const char *src_ptr = src.c_str();

    ceph::bufferlist dst_a;
    ceph::bufferlist dst_b;
    op.write_start = getns_rt();
    for (unsigned i = 0; i < rows_per_obj; i++) {
      if (i % 2 == 0)
        dst_a.append(src_ptr, row_size);
      else
        dst_b.append(src_ptr, row_size);
      src_ptr += row_size;
    }
    assert(dst_a.length() + dst_b.length() == obj_size);

    std::stringstream dst_a_name;
    std::stringstream dst_b_name;

    auto *c_a = librados::Rados::aio_create_completion();
    dst_a_name << oid << "." << run_id << ".0";
    ret = ioctx->aio_write_full(dst_a_name.str(), c_a, dst_a);
    assert(ret == 0);

    auto *c_b = librados::Rados::aio_create_completion();
    dst_b_name << oid << "." << run_id << ".1";
    ret = ioctx->aio_write_full(dst_b_name.str(), c_b, dst_b);
    assert(ret == 0);

    c_a->wait_for_safe();
    assert(c_a->get_return_value() == 0);

    c_b->wait_for_safe();
    assert(c_b->get_return_value() == 0);

    op.write_end = getns_rt();

    c_a->release();
    c_b->release();

    ops.push_back(op);
  }
}

static void worker_generate(librados::IoCtx *ioctx)
{
  // create random data to use for payloads
  const size_t rand_buf_size = 1ULL<<20;
  std::string rand_buf;
  rand_buf.reserve(rand_buf_size);
  std::ifstream ifs("/dev/urandom", std::ios::binary | std::ios::in);
  std::copy_n(std::istreambuf_iterator<char>(ifs),
      rand_buf_size, std::back_inserter(rand_buf));
  const char *rand_buf_raw = rand_buf.c_str();

  // grab random slices from the random bytes
  std::default_random_engine generator;
  std::uniform_int_distribution<int> rand_dist;
  rand_dist = std::uniform_int_distribution<int>(0,
      rand_buf_size - row_size - 1);

  std::string oid;
  while (true) {
    {
      std::lock_guard<std::mutex> l(lock);
      if (src_oids.empty())
        return;
      oid = src_oids.front();
      src_oids.pop_front();
    }

    ceph::bufferlist bl;
    bl.reserve(obj_size);
    for (size_t i = 0; i < rows_per_obj; i++) {
      size_t buf_offset = rand_dist(generator);
      bl.append(rand_buf_raw + buf_offset, row_size);
    }
    assert(bl.length() == obj_size);

    int ret = ioctx->remove(oid);
    assert(ret == 0 || ret == -ENOENT);

    ret = ioctx->write_full(oid, bl);
    checkret(ret, 0);
  }
}

int main(int argc, char **argv)
{
  std::string pool;
  size_t num_objs;
  unsigned nthreads;
  bool generate;
  bool split;
	std::string logfile;
  unsigned seq_start;
  unsigned seq_end;

  po::options_description gen_opts("General options");
  gen_opts.add_options()
    ("help,h", "show help message")
		("pool", po::value<std::string>(&pool)->required(), "pool")
    ("row-size", po::value<size_t>(&row_size)->required(), "row size")
    ("obj-size", po::value<size_t>(&obj_size)->required(), "obj size")
    ("num-objs", po::value<size_t>(&num_objs)->required(), "num objs")
    ("nthreads", po::value<unsigned>(&nthreads)->default_value(1), "num threads")
		("logfile", po::value<std::string>(&logfile)->default_value(""), "log file")

    ("seq-start", po::value<unsigned>(&seq_start)->default_value(0), "seq start")
    ("seq-end", po::value<unsigned>(&seq_end)->default_value(0), "seq end")
    
    ("generate", po::bool_switch(&generate)->default_value(false), "generate mode")
    ("split", po::bool_switch(&split)->default_value(false), "split mode")
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

  // obj size provided in MB
  obj_size = obj_size * (1 << 20);

  assert(row_size > 0);
  assert(obj_size > 0);
  assert(num_objs > 0);
  assert(nthreads > 0);

  // round-up object size to fit row
  if (obj_size % row_size != 0) {
    obj_size = obj_size - obj_size % row_size + row_size;
  }
  assert(obj_size % row_size == 0);

  rows_per_obj = obj_size / row_size;

  if (seq_start == 0 && seq_end == 0) {
    seq_end = num_objs;
  } else {
    assert(seq_end > seq_start);
  }

  std::cout << "row-size: " << row_size << std::endl
            << "obj-size: " << obj_size << std::endl
            << "num-objs: " << num_objs << std::endl
            << "rows/obj: " << rows_per_obj << std::endl
            << "seq-start: " << seq_start << std::endl
            << "seq-end: " << seq_end << std::endl;

  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  checkret(ret, 0);

  // open pool
  librados::IoCtx ioctx;
  ret = cluster.ioctx_create(pool.c_str(), ioctx);
  checkret(ret, 0);

  // generate list of source oids
  for (unsigned i = seq_start; i < seq_end; i++) {
    std::stringstream ss;
    ss << "src." << i;
    src_oids.push_back(ss.str());
  }

  run_id = (unsigned long long)time(NULL);

  std::vector<std::thread> threads;

  if (generate) {
    for (unsigned i = 0; i < nthreads; i++) {
      threads.push_back(std::thread(worker_generate, &ioctx));
    }
  } else if (split) {
    for (unsigned i = 0; i < nthreads; i++) {
      threads.push_back(std::thread(worker_split, &ioctx));
    }
  } else {
    std::cerr << "no mode specified" << std::endl;
  }

  for (auto& thread : threads) {
    thread.join();
  }

  ioctx.close();
  cluster.shutdown();

  if (logfile.length()) {
    std::ofstream out;
    out.open(logfile, std::ios::trunc);
    out << "read_start,write_start,write_end" << std::endl;
		std::lock_guard<std::mutex> l(lock);
    for (const auto& op : results) {
      out <<
        op.read_start << "," <<
        op.write_start << "," <<
        op.write_end << std::endl;
    }
    out.close();
  }
}
