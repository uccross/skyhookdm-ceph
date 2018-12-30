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

  // user/client input, trimmed and encoded to skyhook structs for query_op
  // defaults set below via boost::program_options
  bool index_read;
  bool index_create;
  std::string db_schema;
  std::string table;
  std::string table_schema;
  std::string query_schema;
  std::string index_schema;
  std::string query_preds;
  std::string index_preds;
  std::string index_cols;
  std::string project_cols;

  // set based upon program_options
  int index_type = Tables::SIT_IDX_REC;
  bool fastpath = false;

  // help menu messages for select and project
  std::string query_index_help_msg("Execute query via index lookup. Use " \
                                   "in conjunction with -- select  " \
                                   " and --use-cls flags.");
  std::string project_help_msg("Provide column names as csv list");

  std::string ops_help_msg(" where 'op' is one of: " \
                       "lt, gt, eq, neq, leq, geq, like, in, between, " \
                       "logical_and, logical_or, logical_not, logical_nor, " \
                       "logical_xor, bitwise_and, bitwise_or");
  std::stringstream ss;
  ss.str(std::string());
  ss << "<"
     << "colname" << Tables::PRED_DELIM_INNER
     << "op" << Tables::PRED_DELIM_INNER
     << "value" << Tables::PRED_DELIM_OUTER
     << "colname" << Tables::PRED_DELIM_INNER
     << "op" << Tables::PRED_DELIM_INNER
     << "value" << Tables::PRED_DELIM_OUTER
     << "...>" << ops_help_msg;
  std::string select_help_msg = ss.str();

  std::string create_index_help_msg("To create index on RIDs only, specify '" +
        Tables::RID_INDEX + "', else specify " + project_help_msg +
        ", currently only supports unique indexes over integral columns, with"
        " max number of cols = " + std::to_string(Tables::MAX_COLS_INDEX));

  std::string schema_help_msg = Tables::SCHEMA_FORMAT + "\nEX: \n" +
        Tables::TEST_SCHEMA_STRING_PROJECT;
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
    // query parameters (old)
    ("extended-price", po::value<double>(&extended_price)->default_value(0.0), "extended price")
    ("order-key", po::value<int>(&order_key)->default_value(0.0), "order key")
    ("line-number", po::value<int>(&line_number)->default_value(0.0), "line number")
    ("ship-date-low", po::value<int>(&ship_date_low)->default_value(-9999), "ship date low")
    ("ship-date-high", po::value<int>(&ship_date_high)->default_value(-9999), "ship date high")
    ("discount-low", po::value<double>(&discount_low)->default_value(-9999.0), "discount low")
    ("discount-high", po::value<double>(&discount_high)->default_value(-9999.0), "discount high")
    ("quantity", po::value<double>(&quantity)->default_value(0.0), "quantity")
    ("comment_regex", po::value<std::string>(&comment_regex)->default_value(""), "comment_regex")
    // query parameters (new) flatbufs
    ("db-schema-name", po::value<std::string>(&db_schema)->default_value(Tables::SCHEMA_NAME_DEFAULT), "Database schema name")
    ("table-name", po::value<std::string>(&table)->default_value(Tables::TABLE_NAME_DEFAULT), "Table name")
    ("table-schema", po::value<std::string>(&table_schema)->default_value(Tables::TEST_SCHEMA_STRING), schema_help_msg.c_str())
    ("index-create", po::bool_switch(&index_create)->default_value(false), create_index_help_msg.c_str())
    ("index-read", po::bool_switch(&index_read)->default_value(false), "Use the index for query")
    ("index-cols", po::value<std::string>(&index_cols)->default_value(""), project_help_msg.c_str())
    ("project-cols", po::value<std::string>(&project_cols)->default_value(Tables::PROJECT_DEFAULT), project_help_msg.c_str())
    ("index-preds", po::value<std::string>(&index_preds)->default_value(""), select_help_msg.c_str())
    ("select-preds", po::value<std::string>(&query_preds)->default_value(Tables::SELECT_DEFAULT), select_help_msg.c_str())
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

  } else if (query == "flatbuf") {

    // verify and prep client input
    using namespace Tables;

    // clean input
    boost::trim(db_schema);
    boost::trim(table);
    boost::trim(table_schema);
    boost::trim(index_cols);
    boost::trim(project_cols);
    boost::trim(query_preds);
    boost::trim(index_preds);

    boost::to_upper(db_schema);
    boost::to_upper(table);
    boost::to_upper(index_cols);
    boost::to_upper(project_cols);

    assert (!table.empty());
    assert (!db_schema.empty());
    if(index_create or index_read) {
        assert (!index_cols.empty());
        assert (use_cls);
    }

    // set the index type
    if (index_cols == RID_INDEX)
        index_type = SIT_IDX_RID;
    else
        index_type = SIT_IDX_REC;

    // below we convert user input to skyhook structures for error checking,
    // to be encoded into query_op or index_op structs.

    // verify and set the table schema, needed to create other preds/schemas
    sky_tbl_schema = schemaFromString(table_schema);

    // verify and set the index schema
    sky_idx_schema = schemaFromColNames(sky_tbl_schema, index_cols);

    // verify and set the query predicates
   sky_qry_preds = predsFromString(sky_tbl_schema, query_preds);

    // verify and set the index predicates
    sky_idx_preds = predsFromString(sky_tbl_schema, index_preds);

    // verify and set the query schema, check for select *
    if (project_cols == PROJECT_DEFAULT) {
        for(auto it=sky_tbl_schema.begin();it!=sky_tbl_schema.end();++it) {
            col_info ci(*it);  // deep copy
            sky_qry_schema.push_back(ci);
        }

        // if project all cols and there are no selection preds, set fastpath
        if (sky_qry_preds.size() == 0 and sky_idx_preds.size() == 0)
            fastpath = true;

    } else {
        projection = true;
        if (hasAggPreds(sky_qry_preds)) {
            for (auto it = sky_qry_preds.begin();
                 it != sky_qry_preds.end(); ++it) {
                PredicateBase* p = *it;
                if (p->isGlobalAgg()) {
                    // build col info for agg pred type, append to query schema
                    std::string op_str = skyOpTypeToString(p->opType());
                    int agg_idx = AGG_COL_IDX.at(op_str);
                    int agg_val_type = p->colType();
                    bool is_key = false;
                    bool nullable = false;
                    std::string agg_name = skyOpTypeToString(p->opType());
                    const struct col_info ci(agg_idx, agg_val_type,
                                             is_key, nullable, agg_name);
                    sky_qry_schema.push_back(ci);
                }
            }
        } else {
            sky_qry_schema = schemaFromColNames(sky_tbl_schema, project_cols);
        }
    }

    if (index_read) {
        if (sky_idx_preds.size() > MAX_COLS_INDEX)
            assert (BuildSkyIndexUnsupportedNumCols == 0);
        for (unsigned int i = 0; i < sky_idx_preds.size(); i++) {
            if (sky_idx_preds[i]->opType() != SOT_eq) {
                cerr << "Only equality predicates currently supported for "
                     << "Skyhook indexes" << std::endl;
                assert (SkyIndexUnsupportedOpType == 0);
            }
        }
    }

    // verify index has integral types and check col idx bounds
    if (index_create) {
        if (sky_idx_schema.size() > MAX_COLS_INDEX)
            assert (BuildSkyIndexUnsupportedNumCols == 0);
        for (auto it = sky_idx_schema.begin();
                  it != sky_idx_schema.end(); ++it) {
            col_info ci = *it;
            if (ci.type <= SDT_INT8 or ci.type >= SDT_BOOL)
                assert (BuildSkyIndexUnsupportedColType == 0);
            if (ci.idx <= AGG_COL_LAST)
                assert (BuildSkyIndexUnsupportedAggCol == 0);
            if (ci.idx > static_cast<int>(sky_tbl_schema.size()))
                assert (BuildSkyIndexColIndexOOB == 0);
        }

        // to enforce index is unique, verify that all of the table's keycols
        // are present in the index.
        bool unique_index = true;
        if (index_type == SIT_IDX_REC) {
            for (auto it = sky_tbl_schema.begin();
                      it != sky_tbl_schema.end(); ++it) {
                if (it->is_key) {
                    bool keycol_present = false;
                    for (auto it2 = sky_idx_schema.begin();
                              it2 != sky_idx_schema.end(); ++it2) {
                        if (it->idx==it2->idx) keycol_present = true;
                    }
                    unique_index &= keycol_present;
                }
                if (!unique_index) break;
            }
        }
        assert (unique_index);  // only unique indexes currently supported
    }

    // set all of the flatbuf info for our query op.
    qop_fastpath = fastpath;
    qop_index_read = index_read;
    qop_index_create = index_create;
    qop_index_type = index_type;
    qop_db_schema = db_schema;
    qop_table = table;
    qop_table_schema = schemaToString(sky_tbl_schema);
    qop_query_schema = schemaToString(sky_qry_schema);
    qop_index_schema = schemaToString(sky_idx_schema);
    qop_query_preds = predsToString(sky_qry_preds, sky_tbl_schema);
    qop_index_preds = predsToString(sky_idx_preds, sky_tbl_schema);

  } else {  // query type unknown.
    std::cerr << "invalid query type: " << query << std::endl;
    exit(1);
  }  // end verify query params

  // launch index creation for flatbufs here.
  if (query == "flatbuf" && index_create) {

    idx_op op(true,
              build_index_batch_size,
              index_type,
              qop_index_schema);

    // kick off the workers
    std::vector<std::thread> threads;
    for (int i = 0; i < wthreads; i++) {
      auto ioctx = new librados::IoCtx;
      int ret = cluster.ioctx_create(pool.c_str(), *ioctx);
      checkret(ret, 0);
      threads.push_back(std::thread(worker_build_sky_index, ioctx, op));
    }

    for (auto& thread : threads) {
      thread.join();
    }

    return 0;
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
        op.extra_row_cost = extra_row_cost;
        // flatbufs
        op.fastpath = qop_fastpath;
        op.index_read = qop_index_read;
        op.index_type = qop_index_type;
        op.db_schema = qop_db_schema;
        op.table = qop_table;
        op.table_schema = qop_table_schema;
        op.query_schema = qop_query_schema;
        op.index_schema = qop_index_schema;
        op.query_preds = qop_query_preds;
        op.index_preds = qop_index_preds;
        // cerr << "query_op:" << op.toString() << std::endl;
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
