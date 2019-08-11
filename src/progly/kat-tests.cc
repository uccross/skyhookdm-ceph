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

  std::string pool     = "tpchflatbuf" ;
  std::string oid      = "obj.0" ;
  //SkyFormatType format = SFT_FLATBUF_UNION_ROW ;
  SkyFormatType format = SFT_FLATBUF_UNION_COL ;
  bool debug           = true ;

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

  const std::string KATS_SCHEMA_STRING (" \
    0 " +  std::to_string(Tables::SDT_UINT64) + " 0 0 ATT0 \n\
    1 " +  std::to_string(Tables::SDT_FLOAT) + " 0 0 ATT1 \n\
    2 " +  std::to_string(Tables::SDT_STRING) + " 0 0 ATT2 \n\
    3 " +  std::to_string(Tables::SDT_UINT64) + " 0 0 ATT3 \n\
    ");

  //std::cout << KATS_SCHEMA_STRING << std::endl ;
  //exit(1) ;

  Tables::schema_vec sky_tbl_schema = Tables::schemaFromString( KATS_SCHEMA_STRING ) ;

  std::string project_cols = "att0,att1,att2,att3" ;
  boost::trim( project_cols ) ;
  boost::to_upper( project_cols ) ;
  Tables::schema_vec sky_qry_schema = schemaFromColNames(sky_tbl_schema, project_cols) ;

  std::string query_preds = "" ;
  boost::trim( query_preds ) ;
  Tables::predicate_vec sky_qry_preds = predsFromString( sky_tbl_schema, query_preds ) ;

  while( it_wrapped.get_remaining() > 0 ) {
    if( debug )
      std::cout << "it_wrapped.get_remaining() = " 
                << it_wrapped.get_remaining() << std::endl ;

    // grab the Root
    ceph::bufferlist bl ;
    ::decode( bl, it_wrapped ) ; // this decrements get_remaining by moving iterator
    const char* dataptr = bl.c_str() ;
    size_t datasz       = bl.length() ;

    bool print_header  = true ;
    bool print_verbose = true ;
    int max_to_print   = 10 ;
//    auto ret1 = Tables::printFlatbufFBUAsCsv( 
//                  dataptr, 
//                  datasz, 
//                  print_header, 
//                  print_verbose, 
//                  max_to_print, 
//                  format ) ;
//    std::cout << ret1 << std::endl ;
  
    flatbuffers::FlatBufferBuilder flatbldr( 1024 ) ; // pre-alloc
    std::string errmsg ;
  
    // ROWS
//    std::cout << "calling processSkyFb_fbu_cols..." << std::endl ;
//    int ret2 = processSkyFb_fbu_rows(
//                flatbldr,
//                sky_tbl_schema,
//                sky_qry_schema,
//                sky_qry_preds,
//                dataptr,
//                datasz,
//                errmsg ) ;
//    std::cout << ret2 << std::endl ;

  } //while

  // COLS
  flatbuffers::FlatBufferBuilder flatbldr_cols( 1024 ) ; // pre-alloc
  std::string errmsg_cols ;

  ceph::bufferlist bl ;
  ::decode( bl, it_wrapped ) ; // this decrements get_remaining by moving iterator
  const char* dataptr = bl.c_str() ;
  size_t datasz       = bl.length() ;

  bool print_header  = true ;
  bool print_verbose = true ;
  int max_to_print   = 10 ;

  std::cout << "calling processSkyFb_fbu_cols..." << std::endl ;
  int ret3 = processSkyFb_fbu_cols(
              wrapped_bl_seq,
              flatbldr_cols,
              sky_tbl_schema,
              sky_qry_schema,
              sky_qry_preds,
              dataptr,
              datasz,
              errmsg_cols ) ;
  std::cout << ret3 << std::endl ;


  return 0 ;
}
