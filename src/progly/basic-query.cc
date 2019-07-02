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

#include "cls/tabular/cls_transform_utils.h"
#include "cls/tabular/cls_tabular.h"
#include "cls/tabular/cls_tabular_utils.h"
#include "include/rados/librados.hpp"
#include "query_utils.h"

namespace po = boost::program_options ;

struct spj_query_op {
  std::string oid ;
  std::string pool ;
  std::vector< std::string > select_atts ;
  std::vector< std::string > from_rels ;
  std::vector< std::string > where_preds ;
} ;

void do_query( spj_query_op, std::string ) ;
void do_query_print( spj_query_op q_op ) ;

int main( int argc, char **argv ) {
  std::cout << "in basic-query ..." << std::endl ;

  std::string oid ;
  std::string pool ;
  std::string select_atts_str ;
  std::string from_rels_str ;
  std::string where_preds_str ;

  po::options_description gen_opts( "General options" ) ;

  gen_opts.add_options()
  ("help,h", "show help message")
  ( "oid", po::value<std::string>(&oid)->required(), "oid" )
  ( "pool", po::value<std::string>(&pool)->required(), "pool" )
  ( "select_atts", po::value<std::string>(&select_atts_str)->required(), "select_atts" )
  ( "from_rels", po::value<std::string>(&from_rels_str)->required(), "from_rels" )
  ( "where_preds", po::value<std::string>(&where_preds_str)->required(), "where_preds" ) ;

  po::options_description all_opts("Allowed options");
  all_opts.add(gen_opts);
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, all_opts), vm);
  if (vm.count("help")) {
    std::cout << all_opts << std::endl;
    return 1;
  }
  po::notify(vm);

  // parse inputs
  // https://stackoverflow.com/a/10861816
  std::stringstream ss_select( select_atts_str ) ;
  std::vector< std::string > select_atts ;
  while( ss_select.good() ) {
    std::string substr ;
    std::getline( ss_select, substr, ',' ) ;
    select_atts.push_back( substr ) ;
  }
  std::stringstream ss_from( from_rels_str ) ;
  std::vector< std::string > from_rels ;
  while( ss_from.good() ) {
    std::string substr ;
    std::getline( ss_from, substr, ',' ) ;
    from_rels.push_back( substr ) ;
  }
  std::stringstream ss_where( where_preds_str ) ;
  std::vector< std::string > where_preds ;
  while( ss_where.good() ) {
    std::string substr ;
    std::getline( ss_where, substr, ',' ) ;
    where_preds.push_back( substr ) ;
  }


  std::cout << "==============================" << std::endl ; 
  spj_query_op op ;
  op.oid         = oid ;
  op.pool        = pool ;
  op.select_atts = select_atts ;
  op.from_rels   = from_rels ;
  op.where_preds = where_preds ;
  do_query( op, "query_res" ) ;
  std::cout << "=================================" << std::endl ;

  return 0 ;
}

// ======================== //
//          DO QUERY        //
// ======================== //
void do_query( spj_query_op q_op, std::string qtag ) {
  std::cout << "-----------------------------------------" << std::endl ;
  std::cout << "in do_query..." << std::endl ;
  std::cout << "q_op.oid         : "  << q_op.oid         << std::endl ;
  std::cout << "q_op.pool        : "  << q_op.pool        << std::endl ;
  std::cout << "q_op.select_atts : "  << q_op.select_atts << std::endl ;
  std::cout << "q_op.from_rels   : "  << q_op.from_rels   << std::endl ;
  std::cout << "q_op.where_preds : "  << q_op.where_preds << std::endl ;

  flatbuffers::FlatBufferBuilder res_builder( 1024 ) ;
  librados::bufferlist wrapped_bl_seq ;

  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  checkret(ret, 0);

  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( q_op.pool.c_str(), ioctx ) ;
  checkret( ret, 0 ) ;

  // read bl_seq
  int num_bytes_read = ioctx.read( q_op.oid.c_str(), wrapped_bl_seq, (size_t)0, (uint64_t)0 ) ;
  std::cout << "num_bytes_read : " << num_bytes_read << std::endl ;

  // process data
  ret = process_fb_union( res_builder, wrapped_bl_seq, q_op.select_atts ) ;
  //ceph::bufferlist bl_seq = process_fb_union( res_builder, wrapped_bl_seq, q_op.select_atts ) ;
  checkret( ret, 0 ) ;

  const char* fb = reinterpret_cast<char*>( res_builder.GetBufferPointer() ) ;
  int bufsz      = res_builder.GetSize() ;
  librados::bufferlist bl ;
  bl.append( fb, bufsz ) ;
  std::cout << "do_query bufsz = " << bufsz << std::endl ;

  //// write to flatbuffer
  ceph::bufferlist bl_seq ;
  ::encode( bl, bl_seq ) ;

  // write bl_seq to ceph object
  std::string astr = q_op.oid + "_query" + qtag ;
  const char *obj_name = astr.c_str() ;
  bufferlist::iterator p = bl_seq.begin();
  size_t i = p.get_remaining() ;
  std::cout << "do_query p.get_remaining() = " << i << std::endl ;
  ret = ioctx.write( obj_name, bl_seq, i, 0 ) ;
  checkret(ret, 0);

  ioctx.close() ;
  std::cout << "-----------------------------------------" << std::endl ;

  do_query_print( q_op ) ;
}

// ================= //
//   DO QUERY PRINT  //
// ================= //
void do_query_print( spj_query_op q_op ) {
  std::cout << "-----------------------------------------" << std::endl ;
  std::cout << "in do_query_print ..." << std::endl ;
  std::cout << "q_op.oid         : " << q_op.oid         << std::endl ;
  std::cout << "q_op.pool        : " << q_op.pool        << std::endl ;
  std::cout << "q_op.select_atts : " << q_op.select_atts << std::endl ;
  std::cout << "q_op.from_rels   : " << q_op.from_rels   << std::endl ;
  std::cout << "q_op.where_preds : " << q_op.where_preds << std::endl ;

  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  checkret(ret, 0);

  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( q_op.pool.c_str(), ioctx ) ;
  checkret( ret, 0 ) ;

  // read bl_seq
  librados::bufferlist wrapped_bl_seq ;
  int num_bytes_read = ioctx.read( q_op.oid.c_str(), wrapped_bl_seq, (size_t)0, (uint64_t)0 ) ;
  std::cout << "num_bytes_read : " << num_bytes_read << std::endl ;

  ceph::bufferlist::iterator it_wrapped = wrapped_bl_seq.begin() ;

  // ================================================================================ //
  // display data

  while( it_wrapped.get_remaining() > 0 ) {

    std::cout << "it_wrapped.get_remaining() = " << it_wrapped.get_remaining() << std::endl ;

    // grab the Root
    ceph::bufferlist bl ;
    ::decode( bl, it_wrapped ) ; // this decrements get_remaining by moving iterator
    const char* fb = bl.c_str() ;

    auto root      = Tables::GetRoot( fb ) ;
    auto data_type = root->relationData_type() ;

    std::cout << "data_type : " << data_type << std::endl ;

    // process one Root>Rows flatbuffer
    if( data_type == Tables::Relation_Rows ) {
      std::cout << "if data_type == Tables::Relation_Rows" << std::endl ;

      auto schema    = root->schema() ;
      std::cout << "schema : " << std::endl ;
      for( unsigned int i = 0; i < schema->Length(); i++ )
        std::cout << (unsigned)schema->Get(i) << std::endl ;

      auto rows = static_cast< const Tables::Rows* >( root->relationData() ) ;
      auto table_name_read = rows->table_name() ;

      auto schema_read     = rows->schema() ;
      auto nrows_read      = rows->nrows() ;
      auto ncols_read      = rows->ncols() ;
      auto rids_read       = rows->RIDs() ;
      auto rows_data       = rows->data() ; // [ Record ]

      std::cout << "table_name_read->str() : " << table_name_read->str() << std::endl ;
      std::cout << "schema_read->Length() : " << schema_read->Length() << std::endl ;
      std::cout << "nrows_read     : " << nrows_read     << std::endl ;
      std::cout << "ncols_read     : " << ncols_read     << std::endl ;

      // print schema to stdout
      std::cout << "RID\t" ;
      for( unsigned int i = 0; i < ncols_read; i++ ) {
        std::cout << schema_read->Get(i)->str() << "\t" ;
      }
      std::cout << std::endl ;

      //auto int_data = static_cast< const Tables::IntData* >( rows_data->Get(0) ) ;
      //std::cout << int_data->data()->Get(0) << std::endl ;
      // print data to stdout
      for( unsigned int i = 0; i < rows_data->Length(); i++ ) {
        std::cout << rids_read->Get(i) << ":\t" ;

        auto curr_rec           = rows_data->Get(i) ;
        auto curr_rec_data      = curr_rec->data() ;
        auto curr_rec_data_type = curr_rec->data_type() ;

        //std::cout << "curr_rec_data->Length() = " << curr_rec_data->Length() << std::endl ;

        for( unsigned int j = 0; j < curr_rec_data->Length(); j++ ) {
          // column of ints
          if( (unsigned)curr_rec_data_type->Get(j) == Tables::Data_IntData ) {
            //std::cout << "int" << "\t" ;
            auto int_col_data = static_cast< const Tables::IntData* >( curr_rec_data->Get(j) ) ;
            std::cout << int_col_data->data()->Get(0) << "\t" ;
          }
          // column of floats
          else if( (unsigned)curr_rec_data_type->Get(j) == Tables::Data_FloatData ) {
            //std::cout << "float" << "\t" ;
            auto float_col_data = static_cast< const Tables::FloatData* >( curr_rec_data->Get(j) ) ;
            std::cout << float_col_data->data()->Get(0) << "\t" ;
          }
          // column of strings
          else if( (unsigned)curr_rec_data_type->Get(j) == Tables::Data_StringData ) {
            //std::cout << "str" << "\t" ;
            auto string_col_data = static_cast< const Tables::StringData* >( curr_rec_data->Get(j) ) ;
            std::cout << string_col_data->data()->Get(0)->str() << "\t" ;
          }
          else {
            std::cout << "execute_query: unrecognized row_data_type "
                      << (unsigned)curr_rec_data_type->Get(j) << std::endl ;
          }
        }
        std::cout << std::endl ;
      }
    } // Relation_Rows

    // process one Root>Col flatbuffer
    else if( data_type == Tables::Relation_Col ) {
      std::cout << "else if data_type == Tables::Relation_Col" << std::endl ;

      auto col = static_cast< const Tables::Col* >( root->relationData() ) ;
      auto col_name_read  = col->col_name() ;
      auto col_index_read = col->col_index() ;
      auto nrows_read     = col->nrows() ;
      auto rids_read      = col->RIDs() ;
      auto col_data_type  = col->data_type() ;
      auto col_data       = col->data() ;

      std::cout << "col_name_read->str() : " << col_name_read->str() << std::endl ;
      std::cout << "col_index_read       : " << (unsigned)col_index_read << std::endl ;
      std::cout << "nrows_read           : " << (unsigned)nrows_read     << std::endl ;
      std::cout << "col_data_type        : " << col_data_type     << std::endl ;

      // skip columns not in select_atts
      // trashes query select order
      if( std::find( q_op.select_atts.begin(), 
                     q_op.select_atts.end(), 
                     col_name_read->str() ) == q_op.select_atts.end() &&
          q_op.select_atts.size() > 0 )
        continue ;
      else {
        // print data to stdout
        for( unsigned int i = 0; i < nrows_read; i++ ) {
          std::cout << rids_read->Get(i) << ":\t" ;
          // column of ints
          if( (unsigned)col_data_type == Tables::Data_IntData ) {
            auto int_col_data = static_cast< const Tables::IntData* >( col_data ) ;
            std::cout << int_col_data->data()->Get(i) << "\t" ;
          }
          // column of floats
          else if( (unsigned)col_data_type == Tables::Data_FloatData ) {
            auto float_col_data = static_cast< const Tables::FloatData* >( col_data ) ;
            std::cout << float_col_data->data()->Get(i) << "\t" ;
          }
          // column of strings
          else if( (unsigned)col_data_type == Tables::Data_StringData ) {
            auto string_col_data = static_cast< const Tables::StringData* >( col_data ) ;
            std::cout << string_col_data->data()->Get(i)->str() << "\t" ;
          }
          else {
            std::cout << "unrecognized data_type " << (unsigned)col_data_type << std::endl ;
          }
          std::cout << std::endl ;
        }
      }
    } // Relation_Col

    else {
      std::cout << "unrecognized data_type '" << data_type << "'" << std::endl ;
    }
    std::cout << "loop while" << std::endl ;
  } // while

  ioctx.close() ;
} // EXECUTE QUERY
