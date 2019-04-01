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

  std::string oid = "blah2" ;
  set_rows( oid.c_str() ) ;

  // define query operation
  spj_query_op qo ;
  qo.oid = "blah2" ;
  qo.pool = "tpchflatbuf" ;
  qo.select_atts.push_back( "att1" ) ;
  qo.from_rels.push_back( "atable" ) ;
  qo.where_preds.push_back( "att1<15" ) ;

  // execute query
  execute_query( qo, "ROW" ) ;

  // define transform operation
  transform_op to ;
  to.oid = "blah2" ;
  to.pool = "tpchflatbuf" ;
  to.table_name = "atable" ;
  to.transform_type = "transpose" ;

  // execute transform
  execute_transform( to ) ;

  // query the transpose
  spj_query_op qo1 ;
  qo1.oid = "blah2_transposed" ;
  qo1.pool = "tpchflatbuf" ;
  qo1.select_atts.push_back( "att1" ) ;
  qo1.from_rels.push_back( "atable" ) ;
  qo1.where_preds.push_back( "att1<15" ) ;

  // execute query
  execute_query( qo1, "COL" ) ;

  return 0 ;
}

void set_rows( std::string oid ) {
  std::cout << "in set_rows..." << std::endl ;

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

  flatbuffers::FlatBufferBuilder builder(1024) ;

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

  Tables::RowsBuilder rows_builder( builder ) ;
  rows_builder.add_table_name( table_name ) ;
  rows_builder.add_layout( layout ) ;
  rows_builder.add_schema( schema_fb ) ;
  rows_builder.add_nrows( 3 ) ;
  rows_builder.add_ncols( 2 ) ;
  rows_builder.add_RIDs( rids_vect_fb ) ;
  rows_builder.add_att0( int_vect_fb ) ;
  rows_builder.add_att1( float_vect_fb ) ;

  auto rows = rows_builder.Finish() ;
  builder.Finish( rows ) ;

  //uint8_t *buffer_pointer = builder.GetBufferPointer() ;
  //int size = builder.GetSize() ;
  //auto rows_read = Tables::GetRows( buffer_pointer ) ;
  //auto table_name_read = rows_read->table_name() ;
  //auto schema_read     = rows_read->schema() ;
  //auto nrows_read      = rows_read->nrows() ;
  //auto ncols_read      = rows_read->ncols() ;
  //auto rids_read       = rows_read->RIDs() ;
  //auto att0_read       = rows_read->att0() ;
  //auto att1_read       = rows_read->att1() ;

  //std::cout << "size : " << size << std::endl ;
  //std::cout << "table_name_read : " << table_name_read << std::endl ;
  //std::cout << "schema_read->Length() : " << schema_read->Length() << std::endl ;
  //std::cout << "schema_read->Get( 0 )->str() : " << schema_read->Get( 0 )->str() << std::endl ;
  //std::cout << "schema_read->Get( 1 )->str() : " << schema_read->Get( 1 )->str() << std::endl ;
  //std::cout << "nrows_read  : " << nrows_read << std::endl ;
  //std::cout << "ncols_read  : " << ncols_read << std::endl ;
  //std::cout << "RIDs : " << rids_read << std::endl ;
  //std::cout << rids_read->Length() << std::endl ;
  //std::cout << rids_read->Get(0) << std::endl ;
  //std::cout << rids_read->Get(1) << std::endl ;
  //std::cout << rids_read->Get(2) << std::endl ;

  //std::cout << "att0 : " << att0_read << std::endl ;
  //std::cout << att0_read->Length() << std::endl ;
  //std::cout << att0_read->Get(0) << std::endl ;
  //std::cout << att0_read->Get(1) << std::endl ;
  //std::cout << att0_read->Get(2) << std::endl ;

  //std::cout << "att1 : " << att1_read << std::endl ;
  //std::cout << att1_read->Length() << std::endl ;
  //std::cout << att1_read->Get(0) << std::endl ;
  //std::cout << att1_read->Get(1) << std::endl ;
  //std::cout << att1_read->Get(2) << std::endl ;

  const char* fb = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;
  int bufsz      = builder.GetSize() ;
  librados::bufferlist bl ;
  bl.append( fb, bufsz ) ;

  // write to flatbuffer
  librados::bufferlist bl_seq ;
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
