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

namespace po = boost::program_options ;


void set_rows( std::string ) ;

int main( int argc, char **argv ) {
  std::cout << "in run-transforms..." << std::endl ;

  std::string oid = "atable" ;
  set_rows( oid.c_str() ) ;

  // define query operation
  spj_query_op qo ;
  qo.oid = "atable" ;
  qo.pool = "tpchflatbuf" ;

  // execute query
  std::cout << "ROW query=================================" << std::endl ;
  execute_query( qo ) ;
  std::cout << "=================================" << std::endl ;

  // define transform operation
  transform_op to ;
  to.oid            = "atable" ;
  to.pool           = "tpchflatbuf" ;
  to.transform_type = 0 ; // 0 --> transpose

  // execute transform
  std::cout << "row to col transpose=================================" << std::endl ;
  execute_transform( to ) ;
  std::cout << "=================================" << std::endl ;

  // query the transpose
  spj_query_op qo1 ;
  qo1.oid = "blah2_transposed" ;
  qo1.pool = "tpchflatbuf" ;

  // execute query
  std::cout << "COL query=================================" << std::endl ;
  execute_query( qo1 ) ;
  std::cout << "=================================" << std::endl ;

//  // query the recomposed transpose
//  spj_query_op qo2 ;
//  qo2.oid = "blah2_transposed_transposed" ;
//  qo2.pool = "tpchflatbuf" ;
//
//  // execute query
//  std::cout << "ROW query=================================" << std::endl ;
//  execute_query( qo2, "ROW" ) ;
//  std::cout << "=================================" << std::endl ;

  return 0 ;
}

// writes object called 'oid' to ceph of the form:
// -----------------------------------------------
// | FBMeta flatbuffer     | Rows flatbuffer     |
// -----------------------------------------------
void set_rows( std::string oid ) {
  std::cout << "in set_rows..." << std::endl ;

  flatbuffers::FlatBufferBuilder builder( 1024 ) ;
  librados::bufferlist bl_seq ;

  // --------------------------------------------- //
  // build fb meta bufferlist

  std::vector< uint8_t > meta_schema ;
  meta_schema.push_back( (uint8_t)0 ) ; // 0 --> uint64
  meta_schema.push_back( (uint8_t)1 ) ; // 1 --> float
  auto a = builder.CreateVector( meta_schema ) ;

  std::vector< uint64_t > rids_vect ;
  std::vector< uint64_t > int_vect ;
  std::vector< float > float_vect ;

  rids_vect.push_back( 1 ) ;
  rids_vect.push_back( 2 ) ;
  rids_vect.push_back( 3 ) ;
  int_vect.push_back( 10 ) ;
  int_vect.push_back( 20 ) ;
  int_vect.push_back( 30 ) ;
  float_vect.push_back( 11.5 ) ;
  float_vect.push_back( 21.5 ) ;
  float_vect.push_back( 31.5 ) ;

  std::vector< flatbuffers::Offset< flatbuffers::String > > schema ;
  schema.push_back( builder.CreateString( "att0" ) ) ;
  schema.push_back( builder.CreateString( "att1" ) ) ;

  //place these before record_builder declare
  auto table_name    = builder.CreateString( "atable" ) ;
  auto layout        = builder.CreateString( "ROW" ) ;
  auto schema_fb     = builder.CreateVector( schema ) ;
  auto rids_vect_fb  = builder.CreateVector( rids_vect ) ;
  auto int_vect_fb   = builder.CreateVector( int_vect ) ;
  auto float_vect_fb = builder.CreateVector( float_vect ) ;

  auto iv = Tables::CreateIntData( builder, int_vect_fb ) ;
  auto fv = Tables::CreateFloatData( builder, float_vect_fb ) ;

  std::vector< flatbuffers::Offset< void > > data_vect ;
  data_vect.push_back( iv.Union() ) ;
  data_vect.push_back( fv.Union() ) ;
  auto data = builder.CreateVector( data_vect ) ;

  std::vector< uint8_t > data_type_vect ;
  data_type_vect.push_back( Tables::Data_IntData ) ;
  data_type_vect.push_back( Tables::Data_FloatData ) ;
  auto data_types = builder.CreateVector( data_type_vect ) ;

  // create the Rows flatbuffer:
  auto rows = Tables::CreateRows(
    builder,
    0,
    0,
    table_name,
    schema_fb,
    3,
    2,
    layout,
    rids_vect_fb,
    data_types,
    data ) ;

  Tables::RootBuilder root_builder( builder ) ;
  root_builder.add_schema( a ) ;

  // save the Rows flatbuffer to the root flatbuffer
  root_builder.add_relationData_type( Tables::Relation_Rows ) ;
  root_builder.add_relationData( rows.Union() ) ;

  auto res = root_builder.Finish() ;
  builder.Finish( res ) ;

  const char* fb = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;
  int bufsz      = builder.GetSize() ;
  librados::bufferlist bl ;
  bl.append( fb, bufsz ) ;

  // write to flatbuffer
  //librados::bufferlist bl_seq ;
  ::encode( bl, bl_seq ) ;

  // save to ceph object
  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  checkret(ret, 0);

  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( "tpchflatbuf" , ioctx ) ;
  checkret( ret, 0 ) ;

  // write bl_seq to ceph object
  const char *obj_name = oid.c_str() ;
  bufferlist::iterator p = bl_seq.begin();
  size_t i = p.get_remaining() ;
  std::cout << i << std::endl ;
  ret = ioctx.write( obj_name, bl_seq, i, 0 ) ;
  checkret(ret, 0);

  ioctx.close() ;

  //bl->append( "qwerty" ) ;
  //bl->append( builder ) ;
}
