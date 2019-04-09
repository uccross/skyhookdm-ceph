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
void set_col0( std::string ) ;
void set_col1( std::string ) ;
void set_col2( std::string ) ;

int main( int argc, char **argv ) {
  std::cout << "in run-transforms..." << std::endl ;

// ...................................... //
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
// ...................................... //

// ...................................... //
  std::string oid0 = "test_cols0" ;
  set_col0( oid0.c_str() ) ;

  // define query operation
  spj_query_op qo0 ;
  qo0.oid = "test_cols0" ;
  qo0.pool = "tpchflatbuf" ;

  // execute query
  std::cout << "COL query=================================" << std::endl ;
  execute_query( qo0 ) ;
  std::cout << "=================================" << std::endl ;
// ...................................... //

// ...................................... //
  std::string oid1 = "test_cols1" ;
  set_col1( oid1.c_str() ) ;

  // define query operation
  spj_query_op qo1 ;
  qo1.oid = "test_cols1" ;
  qo1.pool = "tpchflatbuf" ;

  // execute query
  std::cout << "COL query=================================" << std::endl ;
  execute_query( qo1 ) ;
  std::cout << "=================================" << std::endl ;
// ...................................... //

// ...................................... //
  std::string oid2 = "test_cols2" ;
  set_col2( oid2.c_str() ) ;

  // define query operation
  spj_query_op qo2 ;
  qo2.oid = "test_cols2" ;
  qo2.pool = "tpchflatbuf" ;

  // execute query
  std::cout << "COL query=================================" << std::endl ;
  execute_query( qo2 ) ;
  std::cout << "=================================" << std::endl ;
// ...................................... //

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
  spj_query_op print_col0 ;
  print_col0.oid = "atable_transposed" ;
  print_col0.pool = "tpchflatbuf" ;

  // execute query
  std::cout << "COL query=================================" << std::endl ;
  execute_query( print_col0 ) ;
  std::cout << "=================================" << std::endl ;

//  // query the transpose
//  spj_query_op print_col1 ;
//  print_col1.oid = "atable_transposed" ;
//  print_col1.pool = "tpchflatbuf" ;
//
//  // execute query
//  std::cout << "COL query=================================" << std::endl ;
//  execute_query( print_col1 ) ;
//  std::cout << "=================================" << std::endl ;
//
//  // query the transpose
//  spj_query_op print_col2 ;
//  print_col2.oid = "atable_transposed" ;
//  print_col2.pool = "tpchflatbuf" ;
//
//  // execute query
//  std::cout << "COL query=================================" << std::endl ;
//  execute_query( print_col2 ) ;
//  std::cout << "=================================" << std::endl ;

//  // query the recomposed transpose
//  spj_query_op print_col1 ;
//  print_col1.oid = "atable_transposed_transposed" ;
//  print_col1.pool = "tpchflatbuf" ;
//
//  // execute query
//  std::cout << "ROW query=================================" << std::endl ;
//  execute_query( print_col1 ) ;
//  std::cout << "=================================" << std::endl ;
//
//  // query the recomposed transpose
//  spj_query_op print_col2 ;
//  print_col2.oid = "blah2_transposed_transposed" ;
//  print_col2.pool = "tpchflatbuf" ;
//
//  // execute query
//  std::cout << "ROW query=================================" << std::endl ;
//  execute_query( print_col2 ) ;
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

  uint64_t nrows = 4 ;
  uint64_t ncols = 3 ;

  std::vector< uint8_t > meta_schema ;
  meta_schema.push_back( (uint8_t)0 ) ; // 0 --> uint64
  meta_schema.push_back( (uint8_t)1 ) ; // 1 --> float
  auto a = builder.CreateVector( meta_schema ) ;

  std::vector< uint64_t > rids_vect ;
  std::vector< uint64_t > int_vect_raw ;
  std::vector< float > float_vect_raw ;
  std::vector< flatbuffers::Offset<flatbuffers::String> > string_vect_raw ;

  rids_vect.push_back( 1 ) ;
  rids_vect.push_back( 2 ) ;
  rids_vect.push_back( 3 ) ;
  rids_vect.push_back( 4 ) ;
  int_vect_raw.push_back( 10 ) ;
  int_vect_raw.push_back( 20 ) ;
  int_vect_raw.push_back( 30 ) ;
  int_vect_raw.push_back( 40 ) ;
  float_vect_raw.push_back( 11.5 ) ;
  float_vect_raw.push_back( 21.5 ) ;
  float_vect_raw.push_back( 31.5 ) ;
  float_vect_raw.push_back( 41.5 ) ;
  string_vect_raw.push_back( builder.CreateString( "aaaaaaa" ) ) ;
  string_vect_raw.push_back( builder.CreateString( "bbbb" ) ) ;
  string_vect_raw.push_back( builder.CreateString( "blahblahblahblah" ) ) ;
  string_vect_raw.push_back( builder.CreateString( "1234" ) ) ;

  std::vector< flatbuffers::Offset< flatbuffers::String > > schema ;
  schema.push_back( builder.CreateString( "att0" ) ) ;
  schema.push_back( builder.CreateString( "att1" ) ) ;
  schema.push_back( builder.CreateString( "att2" ) ) ;

  //place these before record_builder declare
  auto table_name     = builder.CreateString( "atable" ) ;
  auto layout         = builder.CreateString( "ROW" ) ;
  auto schema_fb      = builder.CreateVector( schema ) ;
  auto rids_vect_fb   = builder.CreateVector( rids_vect ) ;

  std::vector< uint8_t > record_data_type_vect ;
  record_data_type_vect.push_back( Tables::Data_IntData ) ;
  record_data_type_vect.push_back( Tables::Data_FloatData ) ;
  record_data_type_vect.push_back( Tables::Data_StringData ) ;
  auto record_data_types = builder.CreateVector( record_data_type_vect ) ;

  std::vector< flatbuffers::Offset< Tables::Record > > row_records ;

  // create Records as lists of Datas
  for( unsigned int i = 0; i < nrows; i++ ) {

    std::vector< uint64_t > single_iv ;
    std::vector< float > single_fv ;
    std::vector< flatbuffers::Offset<flatbuffers::String> > single_sv ;

    single_iv.push_back( int_vect_raw[ i ] ) ;
    single_fv.push_back( float_vect_raw[ i ] ) ;
    single_sv.push_back( string_vect_raw[ i ] ) ;

    auto int_vect_fb    = builder.CreateVector( single_iv ) ;
    auto float_vect_fb  = builder.CreateVector( single_fv ) ;
    auto string_vect_fb = builder.CreateVector( single_sv ) ;

    // data items
    auto iv = Tables::CreateIntData( builder, int_vect_fb ) ;
    auto fv = Tables::CreateFloatData( builder, float_vect_fb ) ;
    auto sv = Tables::CreateStringData( builder, string_vect_fb ) ;

    // record data
    std::vector< flatbuffers::Offset< void > > data_vect ;
    data_vect.push_back( iv.Union() ) ;
    data_vect.push_back( fv.Union() ) ;
    data_vect.push_back( sv.Union() ) ;
    auto data = builder.CreateVector( data_vect ) ;

    auto rec = Tables::CreateRecord( builder, record_data_types, data ) ;
    row_records.push_back( rec ) ;
  }

  auto row_records_fb = builder.CreateVector( row_records ) ;

  // create the Rows flatbuffer:
  auto rows = Tables::CreateRows(
    builder,
    0,
    0,
    table_name,
    schema_fb,
    nrows,
    ncols,
    layout,
    rids_vect_fb,
    row_records_fb ) ;

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

// writes object called 'oid' to ceph of the form:
// -----------------------------------------------
// | FBMeta flatbuffer     | Col flatbuffer     |
// -----------------------------------------------
void set_col2( std::string oid ) {
  std::cout << "in set_col1..." << std::endl ;

  flatbuffers::FlatBufferBuilder builder( 1024 ) ;
  librados::bufferlist bl_seq ;

  // --------------------------------------------- //
  // build fb meta bufferlist

  std::vector< uint8_t > meta_schema ;
  meta_schema.push_back( (uint8_t)2 ) ; // 2 --> float
  auto a = builder.CreateVector( meta_schema ) ;

  std::vector< uint64_t > rids_vect ;
  std::vector< flatbuffers::Offset<flatbuffers::String> > string_vect ;

  rids_vect.push_back( 1 ) ;
  rids_vect.push_back( 2 ) ;
  rids_vect.push_back( 3 ) ;
  rids_vect.push_back( 4 ) ;
  string_vect.push_back( builder.CreateString( "aaa" ) ) ;
  string_vect.push_back( builder.CreateString( "bbbb" ) ) ;
  string_vect.push_back( builder.CreateString( "blahblahblahblah" ) ) ;
  string_vect.push_back( builder.CreateString( "1234" ) ) ;

  //place these before record_builder declare
  auto col_name       = builder.CreateString( "att1" ) ;
  auto RIDs           = builder.CreateVector( rids_vect ) ;
  auto string_vect_fb = builder.CreateVector( string_vect ) ;
  uint8_t col_index   = 1 ;
  uint8_t nrows       = string_vect.size() ;

  auto data = Tables::CreateStringData( builder, string_vect_fb ) ;

  // create the Rows flatbuffer:
  auto col = Tables::CreateCol(
    builder,
    0,
    0,
    col_name,
    col_index,
    nrows,
    RIDs,
    Tables::Data_StringData,
    data.Union() ) ;

  // save the Rows flatbuffer to the root flatbuffer
  Tables::RootBuilder root_builder( builder ) ;
  root_builder.add_schema( a ) ;
  root_builder.add_relationData_type( Tables::Relation_Col ) ;
  root_builder.add_relationData( col.Union() ) ;

  auto res = root_builder.Finish() ;
  builder.Finish( res ) ;

  const char* fb = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;
  int bufsz      = builder.GetSize() ;
  librados::bufferlist bl ;
  bl.append( fb, bufsz ) ;

  // write to flatbuffer
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
}

// writes object called 'oid' to ceph of the form:
// -----------------------------------------------
// | FBMeta flatbuffer     | Col flatbuffer     |
// -----------------------------------------------
void set_col1( std::string oid ) {
  std::cout << "in set_col1..." << std::endl ;

  flatbuffers::FlatBufferBuilder builder( 1024 ) ;
  librados::bufferlist bl_seq ;

  // --------------------------------------------- //
  // build fb meta bufferlist

  std::vector< uint8_t > meta_schema ;
  meta_schema.push_back( (uint8_t)1 ) ; // 1 --> float
  auto a = builder.CreateVector( meta_schema ) ;

  std::vector< uint64_t > rids_vect ;
  std::vector< float > float_vect ;

  rids_vect.push_back( 1 ) ;
  rids_vect.push_back( 2 ) ;
  rids_vect.push_back( 3 ) ;
  rids_vect.push_back( 4 ) ;
  float_vect.push_back( 1.11111 ) ;
  float_vect.push_back( 2.2 ) ;
  float_vect.push_back( 33333.3 ) ;
  float_vect.push_back( 444.4 ) ;

  //place these before record_builder declare
  auto col_name      = builder.CreateString( "att1" ) ;
  auto RIDs          = builder.CreateVector( rids_vect ) ;
  auto float_vect_fb = builder.CreateVector( float_vect ) ;
  uint8_t col_index = 1 ;
  uint8_t nrows     = float_vect.size() ;

  auto data = Tables::CreateFloatData( builder, float_vect_fb ) ;

  // create the Rows flatbuffer:
  auto col = Tables::CreateCol(
    builder,
    0,
    0,
    col_name,
    col_index,
    nrows,
    RIDs,
    Tables::Data_FloatData,
    data.Union() ) ;

  // save the Rows flatbuffer to the root flatbuffer
  Tables::RootBuilder root_builder( builder ) ;
  root_builder.add_schema( a ) ;
  root_builder.add_relationData_type( Tables::Relation_Col ) ;
  root_builder.add_relationData( col.Union() ) ;

  auto res = root_builder.Finish() ;
  builder.Finish( res ) ;

  const char* fb = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;
  int bufsz      = builder.GetSize() ;
  librados::bufferlist bl ;
  bl.append( fb, bufsz ) ;

  // write to flatbuffer
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
}

// writes object called 'oid' to ceph of the form:
// -----------------------------------------------
// | FBMeta flatbuffer     | Col flatbuffer     |
// -----------------------------------------------
void set_col0( std::string oid ) {
  std::cout << "in set_col0..." << std::endl ;

  flatbuffers::FlatBufferBuilder builder( 1024 ) ;
  librados::bufferlist bl_seq ;

  // --------------------------------------------- //
  // build fb meta bufferlist

  std::vector< uint8_t > meta_schema ;
  meta_schema.push_back( (uint8_t)0 ) ; // 0 --> uint64
  auto a = builder.CreateVector( meta_schema ) ;

  std::vector< uint64_t > rids_vect ;
  std::vector< uint64_t > int_vect ;

  rids_vect.push_back( 1 ) ;
  rids_vect.push_back( 2 ) ;
  rids_vect.push_back( 3 ) ;
  rids_vect.push_back( 4 ) ;
  int_vect.push_back( 10 ) ;
  int_vect.push_back( 20 ) ;
  int_vect.push_back( 30 ) ;
  int_vect.push_back( 40 ) ;

  //place these before record_builder declare
  auto col_name     = builder.CreateString( "att0" ) ;
  auto RIDs         = builder.CreateVector( rids_vect ) ;
  auto int_vect_fb  = builder.CreateVector( int_vect ) ;
  uint8_t col_index = 0 ;
  uint8_t nrows     = int_vect.size() ;

  auto data = Tables::CreateIntData( builder, int_vect_fb ) ;

  // create the Rows flatbuffer:
  auto col = Tables::CreateCol(
    builder,
    0,
    0,
    col_name,
    col_index,
    nrows,
    RIDs,
    Tables::Data_IntData,
    data.Union() ) ;

  // save the Rows flatbuffer to the root flatbuffer
  Tables::RootBuilder root_builder( builder ) ;
  root_builder.add_schema( a ) ;
  root_builder.add_relationData_type( Tables::Relation_Col ) ;
  root_builder.add_relationData( col.Union() ) ;

  auto res = root_builder.Finish() ;
  builder.Finish( res ) ;

  const char* fb = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;
  int bufsz      = builder.GetSize() ;
  librados::bufferlist bl ;
  bl.append( fb, bufsz ) ;

  // write to flatbuffer
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
}
