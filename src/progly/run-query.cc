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
#include <boost/program_options.hpp>
#include "query.h"

namespace po = boost::program_options;

int main(int argc, char **argv)
{
  std::string pool;
  unsigned num_objs;
  int wthreads;
  bool build_index;
  std::string logfile;
  int qdepth;
  std::string dir;

  std::string select_preds_str_default = Tables::SELECT_DEFAULT;
  std::string project_cols_str_default = Tables::PROJECT_DEFAULT;
  std::string schema_str_default = Tables::lineitem_test_schema_string;
  std::string select_preds_str;   // provided by client app
  std::string project_cols_str;  // provided by client app
  std::string schema_str;  // provided by client app

  // TODO: get actual schema from the db client.
  schema_str = schema_str_default;

  // set the table's current schema
  Tables::schema_vec current_schema;
  schemaFromString(current_schema, schema_str);

  // help menu messages for select and project
  std::string project_help_msg = "projected col names as csv list";
  std::stringstream ss;
  ss << " where 'op' is one of: "
     << "lt, gt, eq, neq, leq, geq, like, in, between, "
     << "logical_and, logical_or, logical_not, logical_nor, logical_xor, "
     << "bitwise_and, bitwise_or";
  std::string oplist = ss.str();
  ss.clear();
  ss.str(std::string());
  ss << "<"
     << "colname" << Tables::PRED_DELIM_INNER
     << "op" << Tables::PRED_DELIM_INNER
     << "value" << Tables::PRED_DELIM_OUTER
     << "colname" << Tables::PRED_DELIM_INNER
     << "op" << Tables::PRED_DELIM_INNER
     << "value" << Tables::PRED_DELIM_OUTER
     << "...>" << oplist;
  std::string select_help_msg = ss.str();

  po::options_description gen_opts("General options");
  gen_opts.add_options()
    ("help,h", "show help message")
    ("pool", po::value<std::string>(&pool)->required(), "pool")
    ("num-objs", po::value<unsigned>(&num_objs)->required(), "num objects")
    ("use-cls", po::bool_switch(&use_cls)->default_value(false), "use cls")
    ("quiet,q", po::bool_switch(&quiet)->default_value(false), "quiet")
    ("query", po::value<std::string>(&query)->required(), "query name")
    ("wthreads", po::value<int>(&wthreads)->default_value(1), "num threads")
    ("qdepth", po::value<int>(&qdepth)->default_value(1), "queue depth")
    ("build-index", po::bool_switch(&build_index)->default_value(false), "build index")
    ("use-index", po::bool_switch(&use_index)->default_value(false), "use index")
    ("projection", po::bool_switch(&projection)->default_value(false), "projection")
    ("build-index-batch-size", po::value<uint32_t>(&build_index_batch_size)->default_value(1000), "build index batch size")
    ("extra-row-cost", po::value<uint64_t>(&extra_row_cost)->default_value(0), "extra row cost")
    ("log-file", po::value<std::string>(&logfile)->default_value(""), "log file")
    ("dir", po::value<std::string>(&dir)->default_value("fwd"), "direction")
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
    ("project", po::value<std::string>(&project_cols_str)->default_value(project_cols_str_default), project_help_msg.c_str())
    ("select", po::value<std::string>(&select_preds_str)->default_value(select_preds_str_default), select_help_msg.c_str())
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
  assert(wthreads > 0);
  assert(qdepth > 0);

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

  timings.reserve(num_objs);

  // generate the names of the objects to process
  for (unsigned oidx = 0; oidx < num_objs; oidx++) {
    std::stringstream oid_ss;
    oid_ss << "obj." << oidx;
    const std::string oid = oid_ss.str();
    target_objects.push_back(oid);
  }

  if (dir == "fwd") {
    std::reverse(std::begin(target_objects),
        std::end(target_objects));
  } else if (dir == "bwd") {
    // initial order
  } else if (dir == "rnd") {
    std::random_shuffle(std::begin(target_objects),
        std::end(target_objects));
  } else {
    assert(0);
  }

  // build index for query "d"
  if (build_index) {
    std::vector<std::thread> threads;
    for (int i = 0; i < wthreads; i++) {
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

  } else if (query == "fastpath") {   // no processing required

    assert(!use_index); // not supported
    assert(!projection); // not supported
    std::cout << "select * from lineitem" << std::endl;

  } else if (query == "flatbuf") {   // no processing required

    // the queryop schema string will be either the full current schema
    // or the query's projected schema.
    // set the query schema and check if proj&select

    Tables::schema_vec query_schema;
    Tables::predicate_vec preds;
    Tables::predsFromString(preds, current_schema, select_preds_str);
    boost::trim(project_cols_str);
    boost::trim(project_cols_str_default);

    // check for select *
    if (project_cols_str == project_cols_str_default) {

        // the query schema is identical to the current schema
        for (auto it=current_schema.begin(); it!=current_schema.end(); ++it)
            query_schema.push_back(*it);

        // treat as fastpath query if no project and no select predicates
        if (preds.size() == 0)
            fastpath = true;

    } else {
        projection = true;
        Tables::schemaFromProjectColsString(query_schema, current_schema,
                                            project_cols_str);
        assert(query_schema.size() != 0);
    }

    // to be shipped to cls via query_op struct.
    table_schema_str = schemaToString(current_schema);
    query_schema_str = schemaToString(query_schema);
    predicate_str = predsToString(preds, current_schema);

    // for aggs, set the query output schema correctly here.
    // note output schema will be in same col order as agg preds specified
    if (Tables::hasAggPreds(preds)) {
        query_schema.clear();   // the new return schema will only have aggs
        for (auto it=preds.begin();it!=preds.end();++it) {
            Tables::PredicateBase* p = *it;
            if (p->isGlobalAgg()) {
                // build col info for agg pred type, append to new query schema
                int agg_idx = Tables::agg_idx_names.at(
                        Tables::skyOpTypeToString(p->opType()));
                int agg_val_type = p->colType();
                bool is_key=false;
                bool nullable=false;
                std::string agg_name = Tables::skyOpTypeToString(p->opType());
                const struct Tables::col_info ci(agg_idx, agg_val_type,
                                                 is_key, nullable, agg_name);
                query_schema.push_back(ci);
            }
        }
        query_schema_str = schemaToString(query_schema);
    }

    // cleanup tmp vars
    for (auto it=preds.begin();it!=preds.end();++it)
        delete (*it);
    preds.clear();

  } else {
    std::cerr << "invalid query: " << query << std::endl;
    exit(1);
  }

  result_count = 0;
  rows_returned = 0;
  nrows_processed = 0;
  fastpath |= false;

  outstanding_ios = 0;
  stop = false;

  // start worker threads
  std::vector<std::thread> threads;
  for (int i = 0; i < wthreads; i++) {
    threads.push_back(std::thread(worker));
  }

  std::unique_lock<std::mutex> lock(dispatch_lock);
  while (true) {
    while (outstanding_ios < qdepth) {
      // get an object to process
      if (target_objects.empty())
        break;
      std::string oid = target_objects.back();
      target_objects.pop_back();
      lock.unlock();

      // dispatch an io request
      AioState *s = new AioState;
      s->c = librados::Rados::aio_create_completion(
          s, NULL, handle_cb);

      memset(&s->times, 0, sizeof(s->times));
      s->times.dispatch = getns();

      if (use_cls) {
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
        op.fastpath = fastpath;

        // these are set above during user input err checking
        op.table_schema_str = table_schema_str;
        op.query_schema_str = query_schema_str;
        op.predicate_str = predicate_str;

        op.extra_row_cost = extra_row_cost;
        ceph::bufferlist inbl;
        ::encode(op, inbl);
        int ret = ioctx.aio_exec(oid, s->c,
            "tabular", "query_op", inbl, &s->bl);
        checkret(ret, 0);
      } else {
        int ret = ioctx.aio_read(oid, s->c, &s->bl, 0, 0);
        checkret(ret, 0);
      }

      lock.lock();
      outstanding_ios++;
    }
    if (target_objects.empty())
      break;
    dispatch_cond.wait(lock);
  }
  lock.unlock();

  // drain any still-in-flight operations
  while (true) {
    lock.lock();
    if (outstanding_ios == 0) {
      lock.unlock();
      break;
    }
    lock.unlock();
    std::cout << "draining ios: " << outstanding_ios << " remaining" << std::endl;
    sleep(1);
  }

  // wait for all the workers to stop
  work_lock.lock();
  stop = true;
  work_lock.unlock();
  work_cond.notify_all();

  // the threads will exit when all the objects are processed
  for (auto& thread : threads) {
    thread.join();
  }

  ioctx.close();

  if (query == "a" && use_cls) {
    std::cout << "total result row count: " << result_count
      << " / -1" << "; nrows_processed=" << nrows_processed
      << std::endl;
  } else {
    std::cout << "total result row count: " << result_count
      << " / " << rows_returned  << "; nrows_processed=" << nrows_processed
      << std::endl;
  }

  if (logfile.length()) {
    std::ofstream out;
    out.open(logfile, std::ios::trunc);
    out << "dispatch,response,read_ns,eval_ns,eval2_ns" << std::endl;
    for (const auto& time : timings) {
      out <<
        time.dispatch << "," <<
        time.response << "," <<
        time.read_ns << "," <<
        time.eval_ns << "," <<
        time.eval2_ns << std::endl;
    }
    out.close();
  }

  return 0;
}
