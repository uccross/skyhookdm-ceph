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
  std::string oid_prefix;
  int wthreads;
  bool build_index;
  bool transform_db;
  std::string logfile;
  int qdepth;
  std::string dir;
  std::string conf;

  // user/client input, trimmed and encoded to skyhook structs for query_op
  // defaults set below via boost::program_options
  //bool debug;
  bool index_read;
  bool index_create;
  bool mem_constrain;
  bool text_index_ignore_stopwords;
  bool lock_op;
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
  bool lock_obj_free;
  bool lock_obj_init;
  bool lock_obj_get;
  bool lock_obj_acquire;
  bool lock_obj_create;

  // set based upon program_options
  int index_type = Tables::SIT_IDX_UNK;
  int index2_type = Tables::SIT_IDX_UNK;
  bool fastpath = false;
  bool idx_unique = false;
  bool header = false;  // print csv header

  // example options
  int example_counter;
  int example_function_id;

  // HEP options
  std::string dataset_name;
  std::string file_name;
  std::string tree_name;
  int subpartitions;

  // final output format type for client consumption
  std::string client_format_str;

  // help menu messages for select and project
  std::string query_index_help_msg("Execute query via index lookup. Use " \
        "in conjunction with -- select  and --use-cls flags.");
  std::string project_help_msg("Provide column names as csv list");

  std::string ops_help_msg(" where 'op' is one of: lt, gt, eq, neq, leq, " \
        "geq, like, in, between, logical_and, logical_or, logical_not, " \
        " logical_nor, logical_xor, bitwise_and, bitwise_or");

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
    ("subpartitions", po::value<int>(&subpartitions)->default_value(-1), "maximum num of subpartitions of object names e.g. obj.243.0 and obj.243.1 is one object that has subpartitions=2")
    ("use-cls", po::bool_switch(&use_cls)->default_value(false), "use cls")
    ("quiet,q", po::bool_switch(&quiet)->default_value(false), "quiet")
    ("query", po::value<std::string>(&query)->default_value("flatbuf"), "query name")
    ("wthreads", po::value<int>(&wthreads)->default_value(1), "num threads")
    ("qdepth", po::value<int>(&qdepth)->default_value(1), "queue depth")
    ("build-index", po::bool_switch(&build_index)->default_value(false), "build index")
    ("use-index", po::bool_switch(&use_index)->default_value(false), "use index")
    ("old-projection", po::bool_switch(&old_projection)->default_value(false), "use older projection method")
    ("index-batch-size", po::value<uint32_t>(&index_batch_size)->default_value(1000), "index (read/write) batch size")
    ("extra-row-cost", po::value<uint64_t>(&extra_row_cost)->default_value(0), "extra row cost")
    ("log-file", po::value<std::string>(&logfile)->default_value(""), "log file")
    ("dir", po::value<std::string>(&dir)->default_value("fwd"), "direction")
    ("conf", po::value<std::string>(&conf)->default_value(""), "path to ceph.conf")
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
    // query parameters (new)
    ("debug", po::bool_switch(&debug)->default_value(false), "debug")
    ("table-name", po::value<std::string>(&table_name)->default_value("None"), "Table name")
    ("db-schema-name", po::value<std::string>(&db_schema_name)->default_value(Tables::DBSCHEMA_NAME_DEFAULT), "Database schema name")
    ("data-schema", po::value<std::string>(&data_schema)->default_value(data_schema_example), data_schema_format_help_msg.c_str())
    ("index-create", po::bool_switch(&index_create)->default_value(false), create_index_help_msg.c_str())
    ("index-read", po::bool_switch(&index_read)->default_value(false), "Use the index for query")
    ("mem-constrain", po::bool_switch(&mem_constrain)->default_value(false), "Read/process data structs one at a time within object")
    ("index-cols", po::value<std::string>(&index_cols)->default_value(""), project_help_msg.c_str())
    ("index2-cols", po::value<std::string>(&index2_cols)->default_value(""), project_help_msg.c_str())
    ("project", po::value<std::string>(&project_cols)->default_value(Tables::PROJECT_DEFAULT), project_help_msg.c_str())
    ("index-preds", po::value<std::string>(&index_preds)->default_value(""), select_help_msg.c_str())
    ("index2-preds", po::value<std::string>(&index2_preds)->default_value(""), select_help_msg.c_str())
    ("select", po::value<std::string>(&query_preds)->default_value(Tables::SELECT_DEFAULT), select_help_msg.c_str())
    ("index-delims", po::value<std::string>(&text_index_delims)->default_value(""), "Use delim for text indexes (def=whitespace")
    ("index-ignore-stopwords", po::bool_switch(&text_index_ignore_stopwords)->default_value(false), "Ignore stopwords when building text index. (def=false)")
    ("index-plan-type", po::value<int>(&index_plan_type)->default_value(Tables::SIP_IDX_STANDARD), "If 2 indexes, for intersection plan use '2', for union plan use '3' (def='1')")
    ("runstats", po::bool_switch(&runstats)->default_value(false), "Run statistics on the specified table name")
    ("transform-format-type", po::value<std::string>(&trans_format_str)->default_value("SFT_FLATBUF_FLEX_ROW"), "Destination format type ")
    ("verbose", po::bool_switch(&print_verbose)->default_value(false), "Print detailed record metadata.")
    ("header", po::bool_switch(&header)->default_value(false), "Print row header (i.e., row schema")
    ("limit", po::value<long long int>(&row_limit)->default_value(Tables::ROW_LIMIT_DEFAULT), "SQL limit option, limit num_rows of result set")
    ("example-counter", po::value<int>(&example_counter)->default_value(100), "Loop counter for example function")
    ("example-function-id", po::value<int>(&example_function_id)->default_value(1), "CLS function identifier for example function")
    ("oid-prefix", po::value<std::string>(&oid_prefix)->default_value("obj"), "Prefix to enumerated object ids (names) (def=obj)")
    ("dataset", po::value<std::string>(&dataset_name)->default_value(""), "For HEP data. Not implemented yet.  (def=\"\")")
    ("file", po::value<std::string>(&file_name)->default_value(""), "For HEP data. Not implemented yet.  (def=\"\")")
    ("tree", po::value<std::string>(&tree_name)->default_value(""), "For HEP data. Not implemented yet.  (def=\"\")")
    ("lock-obj-free", po::bool_switch(&lock_obj_free)->default_value(false), "Initialise lock objects")
    ("lock-obj-init", po::bool_switch(&lock_obj_init)->default_value(false), "Initialise table groups")
    ("lock-op", po::bool_switch(&lock_op)->default_value(false), "Use lock mechanism")
    ("lock-obj-get", po::bool_switch(&lock_obj_get)->default_value(false), "Get table values")
    ("lock-obj-acquire", po::bool_switch(&lock_obj_acquire)->default_value(false), "Get table values")
    ("lock-obj-create", po::bool_switch(&lock_obj_create)->default_value(false), "Create Lock obj")
    ("client-format", po::value<std::string>(&client_format_str)->default_value("SFT_ANY"), "Data format type to return to client (def=SFT_ANY)")
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
  if (conf.empty()) {
    cluster.conf_read_file(NULL);
  }
  else {
    cluster.conf_read_file(conf.c_str());
  }
  int ret = cluster.connect();
  checkret(ret, 0);

  // open pool
  librados::IoCtx ioctx;
  ret = cluster.ioctx_create(pool.c_str(), ioctx);
  checkret(ret, 0);
  //~timings.reserve(num_objs);

  // create list of objs to access.
  // start_obj defaults to zero, but start_obj and num_objs can be used to
  // operate on subset ranges of all objects for ops like tranforms or
  // indexing, stats, etc.
  for (unsigned int i = start_obj; i < start_obj+num_objs; i++) {
    if (subpartitions >= 0) {
        for (int j = 0; j < subpartitions; j++) {
            const std::string oid = oid_prefix + "." + table_name + "." + std::to_string(i) + "." + std::to_string(j);
            target_objects.push_back(oid);
        }
    }
    else {
        const std::string oid = oid_prefix + "." + table_name + "." + std::to_string(i);
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
    assert(!old_projection); // not supported
    std::cout << "select * from lineitem" << std::endl;

  } else if (query == "flatbuf") {

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
    boost::trim(trans_format_str);
    boost::trim(client_format_str);

    // standardize naming as uppercase
    boost::to_upper(db_schema_name);
    boost::to_upper(table_name);
    boost::to_upper(index_cols);
    boost::to_upper(index2_cols);
    boost::to_upper(project_cols);
    boost::to_upper(trans_format_str);
    boost::to_upper(client_format_str);

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

    // set and validate the desired format types
    trans_format_type = sky_format_type_from_string(trans_format_str);
    switch (trans_format_type) {
        case SFT_FLATBUF_FLEX_ROW:
        case SFT_ARROW:
        { // these are supported tranformation formats
            break;
        }
        default:
            assert(Tables::TablesErrCodes::EINVALID_TRANSFORM_FORMAT);
    }

    skyhook_output_format = sky_format_type_from_string(client_format_str);
    switch (skyhook_output_format) {
        case SFT_ANY:
        case SFT_CSV:
        case SFT_PG_BINARY:
        case SFT_PYARROW_BINARY:
        { // these are supported final output formats for the client
            break;
        }
        default:
            assert(Tables::TablesErrCodes::EINVALID_OUTPUT_FORMAT);
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

    if (debug) {
        std::cout << "DEBUG: run-query: query predicates:\n";
        for (auto p:sky_qry_preds)
            std::cout << p->toString() << std::endl;
    }

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
    qop_result_format = skyhook_output_format;
    idx_op_idx_unique = idx_unique;
    idx_op_batch_size = index_batch_size;
    idx_op_idx_type = index_type;
    idx_op_idx_schema = schemaToString(sky_idx_schema);
    idx_op_ignore_stopwords = text_index_ignore_stopwords;
    idx_op_text_delims = text_index_delims;
    trans_op_format_type = trans_format_type;

    if (debug) {
        if (query == "flatbuf" || query == "fastpath") {
            cout << "DEBUG: run-query: qop_fastpath=" << qop_fastpath << endl;
            cout << "DEBUG: run-query: qop_index_read=" << qop_index_read << endl;
            cout << "DEBUG: run-query: qop_index_type=" << qop_index_type << endl;
            cout << "DEBUG: run-query: qop_index2_type=" << qop_index2_type << endl;
            cout << "DEBUG: run-query: qop_index_plan_type=" << qop_index_plan_type << endl;
            cout << "DEBUG: run-query: qop_index_batch_size=" << qop_index_batch_size << endl;
            cout << "DEBUG: run-query: qop_db_schema_name=" << qop_db_schema_name << endl;
            cout << "DEBUG: run-query: qop_table_name=" << qop_table_name << endl;
            cout << "DEBUG: run-query: qop_data_schema=\n" << qop_data_schema << endl;
            cout << "DEBUG: run-query: qop_query_schema=\n" << qop_query_schema << endl;
            cout << "DEBUG: run-query: qop_index_schema=\n" << qop_index_schema << endl;
            cout << "DEBUG: run-query: qop_index2_schema=\n" << qop_index2_schema << endl;
            cout << "DEBUG: run-query: qop_query_preds=" << qop_query_preds << endl;
            cout << "DEBUG: run-query: qop_index_preds=" << qop_index_preds << endl;
            cout << "DEBUG: run-query: qop_index2_preds=" << qop_index2_preds << endl;
            cout << "DEBUG: run-query: qop_result_format=" << qop_result_format << endl;
        }
        else if (index_create or index_read) {
            cout << "DEBUG: run-query: idx_op_idx_unique=" << idx_op_idx_unique << endl;
            cout << "DEBUG: run-query: idx_op_batch_size=" << idx_op_batch_size << endl;
            cout << "DEBUG: run-query: idx_op_idx_type=" << idx_op_idx_type << endl;
            cout << "DEBUG: run-query: idx_op_idx_schema=" << idx_op_idx_schema << endl;
            cout << "DEBUG: run-query: idx_op_ignore_stopwords=" << idx_op_ignore_stopwords << endl;
            cout << "DEBUG: run-query: idx_op_text_delims=" << idx_op_text_delims << endl;
        }
    }

    // other processing info
    skyhook_output_format = sky_format_type_from_string(client_format_str);
    if (skyhook_output_format == SFT_PG_BINARY)
        print_header = true;  // binary fstream always requires binary header
    else
        print_header = header;

  } else if (query == "example") {

    /*
      add input error checking here as needed, and
      convert user input to query.h defined values,
      and any other setup needed before encoding the function params
    */

    assert (example_counter >= 0);
    assert (example_function_id >= 0);

    // set client-local output value from user provided boost options
    print_header = header;

    // total result counter from all objs that count using our cls function.
    result_count = example_counter * num_objs;
    cout << "Expect total count " << result_count
         << " from all objects executing example cls method." << std::endl;

    // set example op params from user provided boost options
    expl_func_counter = example_counter;
    expl_func_id = example_function_id;

  } else if (query == "wasm") {

    /*
      add input error checking here as needed, and
      convert user input to query.h defined values,
      and any other setup needed before encoding the function params
    */

    assert (example_counter >= 0);
    assert (example_function_id >= 0);

    // set client-local output value from user provided boost options
    print_header = header;

    // total result counter from all objs that count using our cls function.
    result_count = example_counter * num_objs;
    cout << "Expect total count " << result_count
         << " from all objects executing example cls method." << std::endl;

    // set example op params from user provided boost options
    expl_func_counter = example_counter;
    expl_func_id = example_function_id;

  }
  else {

    // specified query type is unknown.
    std::cerr << "invalid query type: " << query << std::endl;
    exit(1);
  }  // end verify query params*/

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

    if (debug)
        cout << "DEBUG: stats op=" << op.toString() << endl;

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
  if (query == "flatbuf" && transform_db) {

    // create idx_op for workers
    transform_op op(qop_table_name, qop_query_schema, trans_op_format_type);

    if (debug)
        cout << "DEBUG: transform op=" << op.toString() << endl;

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

    if (lock_op) {

        // we only need 1 worker thread to access the single lock obj.
        std::vector<std::thread> threads;
        auto ioctx = new librados::IoCtx;
        int ret = cluster.ioctx_create(pool.c_str(), *ioctx);
        checkret(ret, 0);
        ceph::bufferlist inbl;

        // check which lock-op is set and setup and encode our op params here.
        if (lock_obj_init) {
            lockobj_info op(false, num_objs, table_name,db_schema_name);
            threads.push_back(std::thread(worker_lock_obj_init_op, ioctx, op));
            if (debug)
                cout << "DEBUG: lock_obj_init op=" << op.toString() << endl;
        }
        else if (lock_obj_free){
            lockobj_info op(true, num_objs, table_name,db_schema_name);
            threads.push_back(std::thread(worker_lock_obj_free_op, ioctx, op));
            if (debug)
                cout << "DEBUG: lock_obj_free op=" << op.toString() << endl;
        }
        else if (lock_obj_get){
            lockobj_info op(true, num_objs, table_name,db_schema_name);
            threads.push_back(std::thread(worker_lock_obj_get_op, ioctx, op));
            if (debug)
                cout << "DEBUG: lock_obj_get op=" << op.toString() << endl;
        }
        else if (lock_obj_acquire){
            lockobj_info op(true, num_objs, table_name,db_schema_name);
            threads.push_back(std::thread(worker_lock_obj_acquire_op, ioctx, op));
            if (debug)
                cout << "DEBUG: lock_obj_acquire op=" << op.toString() << endl;
        }
        else if (lock_obj_create){
            lockobj_info op(false, num_objs, table_name,db_schema_name);
            threads.push_back(std::thread(worker_lock_obj_create_op, ioctx, op));
            if (debug)
                cout << "DEBUG: lock_obj_create op=" << op.toString() << endl;
        }
        else {
            cout << "lock_op unknown" << endl;
        }

        for (auto& thread : threads) {
            thread.join();
        }

        return 0;
    }


  /*
   *  Begin launching query op
   *
   */

  result_count = 0;
  rows_returned = 0;
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

      if (query == "flatbuf" ) {
        if (use_cls) {
        query_op op;
        op.debug = debug;
        op.query = query;
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

        if (debug)
            cout << "DEBUG: run-query: launching aio_exec for oid=" << oid << endl;

        // CLS Read
        int ret = ioctx.aio_exec(oid, s->c,
            "tabular", "exec_query_op", inbl, &s->bl);
        checkret(ret, 0);

      } else {

        if (debug)
            cout << "DEBUG: run-query: launching aio_read for oid=" << oid << endl;

        // STD Read
        int ret = ioctx.aio_read(oid, s->c, &s->bl, 0, 0);
        checkret(ret, 0);
      }
  }
    // handle older test queries
    if (query == "a" or
        query == "b" or
        query == "c" or
        query == "d" or
        query == "e" or
        query == "f" or
        query == "g") {

        if (use_cls) {
            test_op op;
            op.query = query;
            op.fastpath = qop_fastpath;
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
            op.old_projection = old_projection;
            op.extra_row_cost = extra_row_cost;
            op.fastpath = fastpath;

            ceph::bufferlist inbl;
            ::encode(op, inbl);
            int ret = ioctx.aio_exec(oid, s->c,
                "tabular", "test_query_op", inbl, &s->bl);
            checkret(ret, 0);
        }
        else {
            int ret = ioctx.aio_read(oid, s->c, &s->bl, 0, 0);
            checkret(ret, 0);
        }
    }

    if (query == "example") {

        if (use_cls) {  // execute a cls read method

            // setup and encode our op params here.
            inbl_sample_op op;
            op.message = "This is an example op";
            op.instructions = "Example instructions";
            op.counter = example_counter;
            op.func_id = example_function_id;
            ceph::bufferlist inbl;
            ::encode(op, inbl);

            // execute our example method on the object, passing in our op.
            int ret = ioctx.aio_exec(oid, s->c,
                "tabular", "example_query_op", inbl, &s->bl);
            checkret(ret, 0);
        }
        else {  // execute standard read
            // read entire object by specifying off=0 len=0.
            int ret = ioctx.aio_read(oid, s->c, &s->bl, 0, 0);
            checkret(ret, 0);
        }
      }

    if (query == "wasm") {

        if (use_cls) {  // execute a cls read method

            // setup and encode our op params here.
            wasm_inbl_sample_op op;
            op.message = "This is an wasm op";
            op.instructions = "Wasm instructions";
            op.counter = example_counter;
            op.func_id = example_function_id;
            ceph::bufferlist inbl;
            ::encode(op, inbl);

            // execute our example method on the object, passing in our op.
            int ret = ioctx.aio_exec(oid, s->c,
                "tabular", "wasm_query_op", inbl, &s->bl);
            checkret(ret, 0);
        }
        else {  // execute standard read
            // read entire object by specifying off=0 len=0.
            int ret = ioctx.aio_read(oid, s->c, &s->bl, 0, 0);
            checkret(ret, 0);
        }
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


  if (stop) {

    // after all objs done processing, if postgres binary fstream,
    // add final trailer to output.

    if ((skyhook_output_format == SkyFormatType::SFT_PG_BINARY) and !quiet) {

        // setup binary stream buf to put trailer
        stringstream ss(std::stringstream::in  |
                        std::stringstream::out |
                        std::stringstream::binary);

        // 16 bit int trailer
        int16_t trailer = -1;
        ss.write(reinterpret_cast<const char*>(&trailer), sizeof(trailer));

        // rewind and output the stream
        ss.seekg (0, ios::beg);
        std::cout << ss.rdbuf();
        ss.flush();
    }

    if (skyhook_output_format == SkyFormatType::SFT_PYARROW_BINARY) {

        // force any remaining output to the pipe
        std::cout << std::flush;
    }
  }

  // only report status messages during quiet operation
  // since otherwise we are printing as csv data to std out
  if (quiet) {
    std::cout << "total result row count: " << result_count << std::endl;
  }


  return 0;
}
