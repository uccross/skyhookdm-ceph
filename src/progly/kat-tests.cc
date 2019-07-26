/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/

#include "include/rados/librados.hpp" // for librados
#include <boost/algorithm/string.hpp> // for boost::trim
#include "cls/tabular/cls_tabular_utils.h"


int main(int argc, char **argv) {

  std::string pool = "tpchflatbuf" ;
  std::string oid  = "blahkat_rows" ;
  bool debug       = true ;

  const std::string KATS_SCHEMA_STRING (" \
    0 " +  std::to_string(Tables::SDT_UINT64) + " 0 0 ATT0 \n\
    1 " +  std::to_string(Tables::SDT_FLOAT) + " 0 0 ATT1 \n\
    2 " +  std::to_string(Tables::SDT_STRING) + " 0 0 ATT2 \n\
    3 " +  std::to_string(Tables::SDT_UINT64) + " 0 0 ATT3 \n\
    ");
  Tables::schema_vec sky_tbl_schema = Tables::schemaFromString( KATS_SCHEMA_STRING ) ;

  std::string project_cols = "att0,att1,att2,att3" ;
  boost::trim( project_cols ) ;
  boost::to_upper( project_cols ) ;
  Tables::schema_vec sky_qry_schema = schemaFromColNames(sky_tbl_schema, project_cols) ;

  std::string query_preds = "" ;
  boost::trim( query_preds ) ;
  Tables::predicate_vec sky_qry_preds = predsFromString( sky_tbl_schema, query_preds ) ;

  flatbuffers::FlatBufferBuilder flatbldr( 1024 ) ; // pre-alloc
  std::string errmsg ;
  const std::vector<uint32_t> row_nums ;

  // --------------------------------- //
  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  checkret( ret, 0 ) ;
  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( pool.c_str(), ioctx ) ;
  checkret( ret, 0 ) ;
  // read bl_seq
  librados::bufferlist wrapped_bl_seq ;
  int num_bytes_read = ioctx.read( oid.c_str(), wrapped_bl_seq, (size_t)0, (uint64_t)0 ) ;
  if( debug )
    std::cout << "num_bytes_read : " << num_bytes_read << std::endl ;
  ceph::bufferlist::iterator it_wrapped = wrapped_bl_seq.begin() ;
  // --------------------------------- //

  while( it_wrapped.get_remaining() > 0 ) {
    if( debug )
      std::cout << "it_wrapped.get_remaining() = " 
                << it_wrapped.get_remaining() << std::endl ;

    // grab the Root
    ceph::bufferlist bl ;
    ::decode( bl, it_wrapped ) ; // this decrements get_remaining by moving iterator
    const char* dataptr = bl.c_str() ;
    size_t datasz       = bl.length() ;
    //auto root           = Tables::GetRoot_FBU( dataptr ) ;
    //auto format         = root->relationData_type() ;
    //int ds_format ;
    //if( format == Tables::Relation_FBU_Rows_FBU )
    //  ds_format = SFT_FLATBUF_UNION_ROWS ;
    //else if( format == Tables::Relation_FBU_Cols_FBU )
    //  ds_format = SFT_FLATBUF_UNION_COLS ;
    //else {
    //  std::cout << "unrecognized format '" << format << "'" << std::endl ;
    //  exit(1) ;
    //}

    //Tables::sky_root ret  = Tables::getSkyRoot( dataptr, datasz, ds_format ) ;
    //Tables::schema_vec sc = Tables::schemaFromString( ret.data_schema ) ;
    //assert( !sc.empty() ) ;

    //if( true )
    //    Tables::printSkyRootHeader( ret ) ;

    //// print header row showing schema
    //if ( true ) {
    //    bool first = true;
    //    for ( Tables::schema_vec::iterator it = sc.begin(); it != sc.end(); ++it ) {
    //        if (!first) std::cout << Tables::CSV_DELIM;
    //        first = false;
    //        std::cout << it->name;
    //        if (it->is_key) std::cout << "(key)";
    //        if (!it->nullable) std::cout << "(NOT NULL)";
    //    }
    //    std::cout << std::endl; // newline to start first row.
    //}

    bool print_header  = true ;
    bool print_verbose = true ;
    int max_to_print   = 4 ;
    auto ret1 = Tables::printFlatbufFBUAsCSV( dataptr, datasz, print_header, print_verbose, max_to_print ) ;
    std::cout << ret1 << std::endl ;

  } //while

  return 0 ;
}
