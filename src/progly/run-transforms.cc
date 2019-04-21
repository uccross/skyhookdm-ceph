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

namespace po = boost::program_options ;

struct spj_query_op {
  std::string oid ;
  std::string pool ;
  std::vector< std::string > select_atts ;
  std::vector< std::string > from_rels ;
  std::vector< std::string > where_preds ;
} ;

void set_rows( std::string ) ;
void set_col0( std::string ) ;
void set_col1( std::string ) ;
void set_col2( std::string ) ;
void execute_query( spj_query_op ) ;

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

// ...................................... //
  // define transform operation
  transform_op to ;
  to.oid            = "atable" ;
  to.pool           = "tpchflatbuf" ;
  to.transform_type = 0 ; // 0 --> transpose
  to.bloffs.push_back( 0 ) ;

  // execute transform
  std::cout << "row to col transpose=================================" << std::endl ;
  librados::bufferlist inbl, outbl ;
  ::encode( to, inbl ) ;

  // connect to rados and open pool
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  checkret(ret, 0);
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( "tpchflatbuf" , ioctx ) ;
  checkret( ret, 0 ) ;
  ret = ioctx.exec( "atable", "tabular", "sky_transform", inbl, outbl ) ;
  bufferlist::iterator p = outbl.begin();
  size_t i = p.get_remaining() ;
  std::cout << i << std::endl ;
  ret = ioctx.write( "atable_transposed", outbl, i, 0 ) ;
  ioctx.close() ;

  std::cout << "=================================" << std::endl ;

  // query the transpose
  spj_query_op print_cols ;
  print_cols.oid = "atable_transposed" ;
  print_cols.pool = "tpchflatbuf" ;

  // execute query
  std::cout << "COL query=================================" << std::endl ;
  execute_query( print_cols ) ;
  std::cout << "=================================" << std::endl ;
// ...................................... //
  // define transform operation
  transform_op to1 ;
  to1.oid            = "atable_transposed" ;
  to1.pool           = "tpchflatbuf" ;
  to1.transform_type = 0 ; // 0 --> transpose
  to1.bloffs.push_back( 0 ) ;
  to1.bloffs.push_back( 1 ) ;
  to1.bloffs.push_back( 2 ) ;
  to1.layout = 1 ;

  // execute transform
  std::cout << "row to col transpose=================================" << std::endl ;
  librados::bufferlist inbl1, outbl1 ;
  ::encode( to1, inbl1 ) ;

  // connect to rados and open pool
  librados::Rados cluster1 ;
  cluster1.init( NULL ) ;
  cluster1.conf_read_file( NULL ) ;
  ret = cluster1.connect() ;
  checkret(ret, 0);
  librados::IoCtx ioctx1 ;
  ret = cluster1.ioctx_create( "tpchflatbuf" , ioctx1 ) ;
  checkret( ret, 0 ) ;
  ret = ioctx1.exec( "atable_transposed", "tabular", "sky_transform", inbl1, outbl1 ) ;
  bufferlist::iterator p1 = outbl1.begin();
  size_t i1 = p1.get_remaining() ;
  std::cout << "i1 = " << i1 << std::endl ;
  ret = ioctx1.write( "atable_transposed_transposed", outbl1, i1, 0 ) ;
  std::cout << "write ret = " << ret << std::endl ;
  ioctx1.close() ;

  std::cout << "=================================" << std::endl ;

  // query the transpose
  spj_query_op print_rows ;
  print_rows.oid = "atable_transposed_transposed" ;
  print_rows.pool = "tpchflatbuf" ;

  // execute query
  std::cout << "ROWS query=================================" << std::endl ;
  execute_query( print_rows ) ;
  std::cout << "=================================" << std::endl ;
// ...................................... //

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

// ============================== //
//          EXECUTE QUERY         //
// ============================== //
void execute_query( spj_query_op q_op ) {
  std::cout << "in execute_query..." << std::endl ;
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
    } // Relation_Col

    else {
      std::cout << "unrecognized data_type '" << data_type << "'" << std::endl ;
    }
  } // while

  ioctx.close() ;
} // EXECUTE QUERY
