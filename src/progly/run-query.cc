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
  unsigned start_obj;
  int wthreads;
  bool build_index;
  bool transform_db;
  std::string logfile;
  int qdepth;
  std::string dir;

  // user/client input, trimmed and encoded to skyhook structs for query_op
  // defaults set below via boost::program_options
  bool index_read;
  bool index_create;
  bool mem_constrain;
  bool text_index_ignore_stopwords;
  int index_plan_type;
  int trans_format_type;
  std::string trans_format_str;
  std::string text_index_delims;
  std::string db_schema_name;
  std::string table_name;
  std::string data_schema;
  std::string query_schema;
  std::string index_schema;
  std::string index2_schema;
  std::string query_preds;
  std::string index_preds;
  std::string index2_preds;
  std::string index_cols;
  std::string index2_cols;
  std::string project_cols;

  // set based upon program_options
  int index_type = Tables::SIT_IDX_UNK;
  int index2_type = Tables::SIT_IDX_UNK;
  bool fastpath = false;
  bool idx_unique = false;
  bool header;  // print csv header
  int resformat;  // format type of result set

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

  std::string data_schema_format_help_msg("NOTE: schema format is: \"col_num  col_type (as SkyDataType enum)  col_is_key col_is_nullable  col_name; col_num col_type ...;\"");
  std::string data_schema_example("0 3 1 0 ORDERKEY ; 1 3 0 1 PARTKEY ; 2 3 0 1 SUPPKEY ; 3 3 1 0 LINENUMBER ; 4 12 0 1 QUANTITY ; 5 13 0 1 EXTENDEDPRICE ; 6 12 0 1 DISCOUNT ; 7 13 0 1 TAX ; 8 9 0 1 RETURNFLAG ; 9 9 0 1 LINESTATUS ; 10 14 0 1 SHIPDATE ; 11 14 0 1 COMMITDATE ; 12 14 0 1 RECEIPTDATE ; 13 15 0 1 SHIPINSTRUCT ; 14 15 0 1 SHIPMODE ; 15 15 0 1 COMMENT");

  std::string create_index_help_msg("To create index on RIDs only, specify '" +
        Tables::RID_INDEX + "', else specify " + project_help_msg +
        ", currently only supports unique indexes over integral columns, with"
        " max number of cols = " + std::to_string(Tables::MAX_INDEX_COLS));

  po::options_description gen_opts("General options");
  gen_opts.add_options()
    ("help,h", "show help message")
    ("pool", po::value<std::string>(&pool)->required(), "pool")
    ("num-objs", po::value<unsigned>(&num_objs)->required(), "num objects")
    ("start-obj", po::value<unsigned>(&start_obj)->default_value(0), "start object (for transform operation")
    ("use-cls", po::bool_switch(&use_cls)->default_value(false), "use cls")
    ("quiet,q", po::bool_switch(&quiet)->default_value(false), "quiet")
    ("query", po::value<std::string>(&query)->required(), "query name")
    ("wthreads", po::value<int>(&wthreads)->default_value(1), "num threads")
    ("qdepth", po::value<int>(&qdepth)->default_value(1), "queue depth")
    ("build-index", po::bool_switch(&build_index)->default_value(false), "build index")
    ("use-index", po::bool_switch(&use_index)->default_value(false), "use index")
    ("projection", po::bool_switch(&projection)->default_value(false), "projection")
    ("index-batch-size", po::value<uint32_t>(&index_batch_size)->default_value(1000), "index (read/write) batch size")
    ("extra-row-cost", po::value<uint64_t>(&extra_row_cost)->default_value(0), "extra row cost")
    ("log-file", po::value<std::string>(&logfile)->default_value(""), "log file")
    ("dir", po::value<std::string>(&dir)->default_value("fwd"), "direction")
    ("transform-db", po::bool_switch(&transform_db)->default_value(false), "transform DB")
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
    ("db-schema-name", po::value<std::string>(&db_schema_name)->default_value(Tables::DBSCHEMA_NAME_DEFAULT), "Database schema name")
    ("table-name", po::value<std::string>(&table_name)->default_value(Tables::TABLE_NAME_DEFAULT), "Table name")
    ("data-schema", po::value<std::string>(&data_schema)->default_value(data_schema_example), data_schema_format_help_msg.c_str())
    ("index-create", po::bool_switch(&index_create)->default_value(false), create_index_help_msg.c_str())
    ("index-read", po::bool_switch(&index_read)->default_value(false), "Use the index for query")
    ("mem-constrain", po::bool_switch(&mem_constrain)->default_value(false), "Read/process data structs one at a time within object")
    ("index-cols", po::value<std::string>(&index_cols)->default_value(""), project_help_msg.c_str())
    ("index2-cols", po::value<std::string>(&index2_cols)->default_value(""), project_help_msg.c_str())
    ("project-cols", po::value<std::string>(&project_cols)->default_value(Tables::PROJECT_DEFAULT), project_help_msg.c_str())
    ("index-preds", po::value<std::string>(&index_preds)->default_value(""), select_help_msg.c_str())
    ("index2-preds", po::value<std::string>(&index2_preds)->default_value(""), select_help_msg.c_str())
    ("select-preds", po::value<std::string>(&query_preds)->default_value(Tables::SELECT_DEFAULT), select_help_msg.c_str())
    ("index-delims", po::value<std::string>(&text_index_delims)->default_value(""), "Use delim for text indexes (def=whitespace")
    ("index-ignore-stopwords", po::bool_switch(&text_index_ignore_stopwords)->default_value(false), "Ignore stopwords when building text index. (def=false)")
    ("index-plan-type", po::value<int>(&index_plan_type)->default_value(Tables::SIP_IDX_STANDARD), "If 2 indexes, for intersection plan use '2', for union plan use '3' (def='1')")
    ("runstats", po::bool_switch(&runstats)->default_value(false), "Run statistics on the specified table name")
    ("transform-format-type", po::value<std::string>(&trans_format_str)->default_value("flatbuffer"), "Destination format type ")
    ("verbose", po::bool_switch(&print_verbose)->default_value(false), "Print detailed record metadata.")
    ("header", po::bool_switch(&header)->default_value(true), "Print csv row header.")
    ("limit", po::value<long long int>(&row_limit)->default_value(Tables::ROW_LIMIT_DEFAULT), "SQL limit option, limit num_rows of result set")
    ("result-format", po::value<int>(&resformat)->default_value((int)SkyFormatType::SFT_FLATBUF_FLEX_ROW), "desired SkyFormatType (enum) of results")

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

  {
      unsigned oidx = 0;
      if (transform_db) {
          oidx = start_obj;
          num_objs += start_obj;
      }

      // generate the names of the objects to process
      for (; oidx < num_objs; oidx++) {
          std::stringstream oid_ss;
          oid_ss << "obj." << oidx;
          const std::string oid = oid_ss.str();
          target_objects.push_back(oid);
      }
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

  // Get the destination object type for the transform operation
  if (trans_format_str == "flatbuffer") {
    trans_format_type = SFT_FLATBUF_FLEX_ROW;
  } else if (trans_format_str == "arrow") {
    trans_format_type = SFT_ARROW;
  } else {
    assert(0);
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

  } else if (query == "flatbuf" || query == "arrow") {

    // verify and prep client input
    using namespace Tables;

    // clean input
    boost::trim(db_schema_name);
    boost::trim(table_name);
    boost::trim(data_schema);
    boost::trim(index_cols);
    boost::trim(index2_cols);
    boost::trim(project_cols);
    boost::trim(query_preds);
    boost::trim(index_preds);
    boost::trim(index2_preds);
    boost::trim(text_index_delims);

    // standardize naming as uppercase
    boost::to_upper(db_schema_name);
    boost::to_upper(table_name);
    boost::to_upper(index_cols);
    boost::to_upper(index2_cols);
    boost::to_upper(project_cols);

    // current minimum required info for formulating IO requests.
    assert (!db_schema_name.empty());
    assert (!table_name.empty());
    assert (!data_schema.empty());

    // verify compatible index/stats options
    if(index_create or index_read) {
        assert (!index_cols.empty());
        assert (use_cls);
    }
    if (runstats) {
        assert (use_cls);
    }

     // verify desired result format is supported
    switch (resformat) {
        case SFT_FLATBUF_FLEX_ROW:
            break;
        case SFT_FLATBUF_UNION_ROW:
        case SFT_FLATBUF_UNION_COL:
        case SFT_FLATBUF_CSV_ROW:
        case SFT_ARROW:
        case SFT_PG_TUPLE:
        case SFT_CSV:
        default:
            assert (SkyFormatTypeNotImplemented==0);
    }

    // below we convert user input to skyhook structures for error checking,
    // to be encoded into query_op or index_op structs.

    // verify and set the table schema, needed to create other preds/schemas
    sky_tbl_schema = schemaFromString(data_schema);

    // verify and set the index schema
    sky_idx_schema = schemaFromColNames(sky_tbl_schema, index_cols);
    sky_idx2_schema = schemaFromColNames(sky_tbl_schema, index2_cols);

    // verify and set the query predicates
    sky_qry_preds = predsFromString(sky_tbl_schema, query_preds);

    // verify and set the index predicates
    sky_idx_preds = predsFromString(sky_tbl_schema, index_preds);
    sky_idx2_preds = predsFromString(sky_tbl_schema, index2_preds);

    // verify and set the query schema, check for select *
    if (project_cols == PROJECT_DEFAULT) {
        for(auto it=sky_tbl_schema.begin(); it!=sky_tbl_schema.end(); ++it) {
            col_info ci(*it);  // deep copy
            sky_qry_schema.push_back(ci);
        }

        // if project all cols and there are no selection preds, set fastpath
        if (sky_qry_preds.size() == 0 and
            sky_idx_preds.size() == 0 and
            sky_idx2_preds.size() == 0) {
                fastpath = true;
        }

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

    // set the index type
    if (!index_cols.empty()) {
        if (index_cols == RID_INDEX) { // const value for colname=RID
            index_type = SIT_IDX_RID;
        }
        else if (sky_idx_schema.size() == 1 and
                 sky_idx_schema.at(0).type == SDT_STRING) {
                    index_type = SIT_IDX_TXT;  // txt indexes are 1 string col
        }
        else {
            index_type = SIT_IDX_REC;
        }
    }

    // set the index2 type
    if (!index2_cols.empty()) {
        if (index2_cols == RID_INDEX) { // const value for colname=RID
            index2_type = SIT_IDX_RID;
        }
        else if (sky_idx2_schema.size() == 1 and
                 sky_idx2_schema.at(0).type == SDT_STRING) {
                    index2_type = SIT_IDX_TXT;  // txt indexes are 1 string col
        }
        else {
            index2_type = SIT_IDX_REC;
        }
    }

    if (index_type == SIT_IDX_UNK or index2_type == SIT_IDX_UNK)
        index_plan_type = SIP_IDX_STANDARD;

    assert ((index_plan_type == SIP_IDX_STANDARD) or
            (index_plan_type == SIP_IDX_INTERSECTION) or
            (index_plan_type == SIP_IDX_UNION));

    // verify index predicates: op type supported and if all index
    // predicate cols are in the specified index.
    if (index_read) {
        if (sky_idx_preds.size() > MAX_INDEX_COLS)
            assert (BuildSkyIndexUnsupportedNumCols == 0);
        for (unsigned int i = 0; i < sky_idx_preds.size(); i++) {
            switch (sky_idx_preds[i]->opType()) {
                case SOT_gt:
                case SOT_lt:
                case SOT_eq:
                case SOT_leq:
                case SOT_geq:
                    break;  // all ok, supported index ops
                default:
                    cerr << "Only >, <, =, <=, >= predicates currently "
                         << "supported for Skyhook indexes" << std::endl;
                    assert (SkyIndexUnsupportedOpType == 0);
            }
            // verify index pred cols are all in the index schema
            bool found = false;
            for (unsigned j = 0; j < sky_idx_schema.size() and !found; j++) {
                found |= (sky_idx_schema[j].idx == sky_idx_preds[i]->colIdx());
            }
            if (!found) {
                cerr << "Index predicate cols are not all present in the "
                     << "specified index:(" << index_cols << ")" << std::endl;
                assert (SkyIndexColNotPresent == 0);
            }
        }
        if (sky_idx2_preds.size() > MAX_INDEX_COLS)
            assert (BuildSkyIndexUnsupportedNumCols == 0);
        for (unsigned int i = 0; i < sky_idx2_preds.size(); i++) {
            switch (sky_idx2_preds[i]->opType()) {
                case SOT_gt:
                case SOT_lt:
                case SOT_eq:
                case SOT_leq:
                case SOT_geq:
                    break;  // all ok, supported index ops
                default:
                    cerr << "Only >, <, =, <=, >= predicates currently "
                         << "supported for Skyhook indexes" << std::endl;
                    assert (SkyIndexUnsupportedOpType == 0);
            }
            // verify index pred cols are all in the index schema
            bool found = false;
            for (unsigned j = 0; j < sky_idx2_schema.size() and !found; j++) {
                found |= (sky_idx2_schema[j].idx == sky_idx2_preds[i]->colIdx());
            }
            if (!found) {
                cerr << "Index predicate cols are not all present in the "
                     << "specified index:(" << index2_cols << ")" << std::endl;
                assert (SkyIndexColNotPresent == 0);
            }
        }

    }

    // verify index types are integral/string and check col idx bounds
    if (index_create) {
        if (index_type == SIT_IDX_TXT) {
            if (sky_idx_schema.size() > 1)  // enforce TXT indexes are 1 column
                assert (BuildSkyIndexUnsupportedNumCols == 0);
        }
        if (sky_idx_schema.size() > MAX_INDEX_COLS)
            assert (BuildSkyIndexUnsupportedNumCols == 0);
        for (auto it = sky_idx_schema.begin();
                  it != sky_idx_schema.end(); ++it) {
            col_info ci = *it;
            if (index_type == SIT_IDX_TXT) {  // txt indexes only string cols
                if(ci.type != SDT_STRING)
                    assert (BuildSkyIndexUnsupportedColType == 0);
            }
            else if (index_type == SIT_IDX_REC or index_type == SIT_IDX_RID) {
                if (ci.type < SDT_INT8 or ci.type > SDT_BOOL)
                    assert (BuildSkyIndexUnsupportedColType == 0);
            }
            if (ci.idx <= AGG_COL_LAST and ci.idx != RID_COL_INDEX)
                assert (BuildSkyIndexUnsupportedAggCol == 0);
            if (ci.idx > static_cast<int>(sky_tbl_schema.size()))
                assert (BuildSkyIndexColIndexOOB == 0);
        }

        // check for index uniqueness, only used for build index,
        // not read index so we do not check the index2 here.
        idx_unique = true;
        if (index_type == SIT_IDX_REC or index_type == SIT_IDX_TXT) {
            for (auto it = sky_tbl_schema.begin();
                      it != sky_tbl_schema.end(); ++it) {
                if (it->is_key) {
                    bool keycol_present = false;
                    for (auto it2 = sky_idx_schema.begin();
                              it2 != sky_idx_schema.end(); ++it2) {
                        if (it->idx==it2->idx) keycol_present = true;
                    }
                    idx_unique &= keycol_present;
                }
                if (!idx_unique)
                    break;
            }
        }
    }

    // set all of the flatbuf info for our query op.
    qop_fastpath = fastpath;
    qop_index_read = index_read;
    qop_mem_constrain = mem_constrain;
    qop_index_type = index_type;
    qop_index2_type = index2_type;
    qop_index_plan_type = index_plan_type;
    qop_index_batch_size = index_batch_size;
    qop_db_schema_name = db_schema_name;
    qop_table_name = table_name;
    qop_data_schema = schemaToString(sky_tbl_schema);
    qop_query_schema = schemaToString(sky_qry_schema);
    qop_index_schema = schemaToString(sky_idx_schema);
    qop_index2_schema = schemaToString(sky_idx2_schema);
    qop_query_preds = predsToString(sky_qry_preds, sky_tbl_schema);
    qop_index_preds = predsToString(sky_idx_preds, sky_tbl_schema);
    qop_index2_preds = predsToString(sky_idx2_preds, sky_tbl_schema);
    qop_result_format = resformat;
    idx_op_idx_unique = idx_unique;
    idx_op_batch_size = index_batch_size;
    idx_op_idx_type = index_type;
    idx_op_idx_schema = schemaToString(sky_idx_schema);
    idx_op_ignore_stopwords = text_index_ignore_stopwords;
    idx_op_text_delims = text_index_delims;
    trans_op_format_type = trans_format_type;

  } else {  // query type unknown.
    std::cerr << "invalid query type: " << query << std::endl;
    exit(1);
  }  // end verify query params

  // launch index creation on given table and cols here.
  if (query == "flatbuf" && index_create) {

    // create idx_op for workers
    idx_op op(idx_op_idx_unique,
              idx_op_ignore_stopwords,
              idx_op_batch_size,
              idx_op_idx_type,
              idx_op_idx_schema,
              idx_op_text_delims);

    // kick off the workers
    std::vector<std::thread> threads;
    for (int i = 0; i < wthreads; i++) {
      auto ioctx = new librados::IoCtx;
      int ret = cluster.ioctx_create(pool.c_str(), *ioctx);
      checkret(ret, 0);
      threads.push_back(std::thread(worker_exec_build_sky_index_op, ioctx, op));
    }

    for (auto& thread : threads) {
      thread.join();
    }

    return 0;
  }


  // launch run statistics on given table here.
  if (query == "flatbuf" && runstats) {

    // create idx_op for workers
    stats_op op(qop_db_schema_name, qop_table_name, qop_data_schema);

    // kick off the workers
    std::vector<std::thread> threads;
    for (int i = 0; i < wthreads; i++) {
      auto ioctx = new librados::IoCtx;
      int ret = cluster.ioctx_create(pool.c_str(), *ioctx);
      checkret(ret, 0);
      threads.push_back(std::thread(worker_exec_runstats_op, ioctx, op));
    }

    for (auto& thread : threads) {
      thread.join();
    }

    return 0;
  }

  // launch transform operation here.
  if (transform_db) {

    // create idx_op for workers
    transform_op op(qop_table_name, qop_data_schema, trans_op_format_type);

    // kick off the workers
    std::vector<std::thread> threads;
    for (int i = 0; i < wthreads; i++) {
      auto ioctx = new librados::IoCtx;
      int ret = cluster.ioctx_create(pool.c_str(), *ioctx);
      checkret(ret, 0);
      threads.push_back(std::thread(worker_transform_db_op, ioctx, op));
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
  print_header = header;  // used for csv printing

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
        op.mem_constrain = qop_mem_constrain;
        op.index_type = qop_index_type;
        op.index2_type = qop_index2_type;
        op.index_plan_type = qop_index_plan_type;
        op.index_batch_size = qop_index_batch_size;
        op.result_format = qop_result_format;
        op.db_schema_name = qop_db_schema_name;
        op.table_name = qop_table_name;
        op.data_schema = qop_data_schema;
        op.query_schema = qop_query_schema;
        op.index_schema = qop_index_schema;
        op.index2_schema = qop_index2_schema;
        op.query_preds = qop_query_preds;
        op.index_preds = qop_index_preds;
        op.index2_preds = qop_index2_preds;
        ceph::bufferlist inbl;
        ::encode(op, inbl);
        int ret = ioctx.aio_exec(oid, s->c,
            "tabular", "exec_query_op", inbl, &s->bl);
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

    // only report status messages during quiet operation
    // since otherwise we are printing as csv data to std out
    if (quiet)
        std::cout << "draining ios: " << outstanding_ios << " remaining\n";
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

  // only report status messages during quiet operation
  // since otherwise we are printing as csv data to std out
  if (quiet) {
      if (query == "a" && use_cls) {
        std::cout << "total result row count: " << result_count << " / -1"
                  << "; nrows_processed=" << nrows_processed << std::endl;
      } else {
        std::cout << "total result row count: " << result_count << " / "
                  << rows_returned  << "; nrows_processed=" << nrows_processed
                  << std::endl;
      }
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
