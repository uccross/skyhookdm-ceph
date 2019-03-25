/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#include "cls_transform_utils.h"
#include "include/rados/librados.hpp"

#include "rows_generated.h"

void test() {
  std::cout << "blee" << std::endl ;

  // write to flatbuffer
  librados::bufferlist bl_seq ;

  auto bl_rows = set_rows() ;
  ::encode( bl_rows, bl_seq ) ;

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

  // write bl_seq
  const char *obj_name  = "blah" ;
  bufferlist::iterator p = bl_seq.begin();
  size_t i = p.get_remaining() ;
  std::cout << i << std::endl ;
  ret = ioctx.write( obj_name, bl_seq, i, 0 ) ;
  checkret(ret, 0);

  // read bl_seq
  librados::bufferlist wrapped_bl_seq ;
  int num_bytes_read = ioctx.read( obj_name, wrapped_bl_seq, (size_t)0, (uint64_t)0 ) ;

  ceph::bufferlist::iterator it_wrapped = wrapped_bl_seq.begin() ;
  std::cout << it_wrapped.get_remaining() << std::endl ;

  // ================================================================================ //
  // display Rows data

  ceph::bufferlist bl ;
  ::decode( bl, it_wrapped ) ; // this decrements get_remaining by moving iterator

  const char* fb = bl.c_str() ;
  auto root = Tables::GetRows( fb ) ;

  auto table_name = root->table_name() ;
  auto rids_read = root->RIDs() ;
  auto att0_read = root->att0() ;
  auto att1_read = root->att1() ;

  std::cout << "table_name : " << table_name << std::endl ;
  std::cout << "RIDs : " << rids_read->Length() << std::endl ;
  std::cout << "att0 : " << att0_read->Length() << std::endl ;
  std::cout << "att1 : " << att1_read->Length() << std::endl ;

  // ================================================================================ //
  // do row to col tranpose

  auto transposed_bl_seq = transpose( wrapped_bl_seq ) ;

  ceph::bufferlist::iterator it_transposed = transposed_bl_seq.begin() ;
  std::cout << it_transposed.get_remaining() << std::endl ;

  ceph::bufferlist ints_bl ;
  ::decode( ints_bl, it_transposed ) ; // this decrements get_remaining by moving iterator
  const char* ints_fb = ints_bl.c_str() ;
  auto ints_root      = Tables::GetCols_int( ints_fb ) ;

  auto int_att0_read = root->att0() ;
  std::cout << "int_att0_read->Length() : " << int_att0_read->Length() << std::endl ;
  std::cout << int_att0_read->Get( 0 ) << std::endl ;
  std::cout << int_att0_read->Get( 1 ) << std::endl ;
  std::cout << int_att0_read->Get( 2 ) << std::endl ;

  ceph::bufferlist floats_bl ;
  ::decode( floats_bl, it_transposed ) ; // this decrements get_remaining by moving iterator
  const char* floats_fb = floats_bl.c_str() ;
  auto floats_root      = Tables::GetCols_float( floats_fb ) ;

  auto float_att0_read = floats_root->att0() ;
  std::cout << "float_att0_read->Length() : " << float_att0_read->Length() << std::endl ;
  std::cout << float_att0_read->Get( 0 ) << std::endl ;
  std::cout << float_att0_read->Get( 1 ) << std::endl ;
  std::cout << float_att0_read->Get( 2 ) << std::endl ;

  ioctx.close() ;
}

librados::bufferlist transpose( librados::bufferlist wrapped_bl_seq ) {
  std::cout << "bloo" << std::endl ;

  librados::bufferlist transposed_bl_seq ;
  ceph::bufferlist::iterator it_wrapped = wrapped_bl_seq.begin() ;

  ceph::bufferlist bl ;
  ::decode( bl, it_wrapped ) ;
  const char* fb = bl.c_str() ;
  auto root = Tables::GetRows( fb ) ;

  auto table_name  = root->table_name() ;
  auto rids_read   = root->RIDs() ;
  auto ints_read   = root->att0() ;
  auto floats_read = root->att1() ;

  std::vector< uint64_t > rids_vect ; 
  for( unsigned int i = 0; i < rids_read->Length(); i++ ) {
    rids_vect.push_back( rids_read->Get( i ) ) ;
  }
  std::vector< uint64_t > ints_vect ; 
  for( unsigned int i = 0; i < ints_read->Length(); i++ ) {
    ints_vect.push_back( ints_read->Get( i ) ) ;
  }
  std::vector< float > floats_vect ; 
  for( unsigned int i = 0; i < floats_read->Length(); i++ ) {
    floats_vect.push_back( floats_read->Get( i ) ) ;
  }

  // ----------------------------------------------- //
  // build int column

  flatbuffers::FlatBufferBuilder ints_builder(1024) ;

  //place these before record_builder declare
  auto i_rids_vect_fb = ints_builder.CreateVector( rids_vect ) ;
  auto ints_vect_fb   = ints_builder.CreateVector( ints_vect ) ;

  Tables::Cols_intBuilder cols_int_builder( ints_builder ) ;
  cols_int_builder.add_RIDs( i_rids_vect_fb ) ;
  cols_int_builder.add_att0( ints_vect_fb ) ;

  auto cols_int = cols_int_builder.Finish() ;
  ints_builder.Finish( cols_int ) ;

  // save record in bufferlist bl
  const char* ints_fb = reinterpret_cast<char*>( ints_builder.GetBufferPointer() ) ;

  int int_bfsz = ints_builder.GetSize() ;
  librados::bufferlist bl_ints ;
  bl_ints.append( ints_fb, int_bfsz ) ;

  // append to transposed_bl_seq bufferlist
  ::encode( bl_ints, transposed_bl_seq ) ;

  // ----------------------------------------------- //
  // build float column

  flatbuffers::FlatBufferBuilder floats_builder(1024) ;

  //place these before record_builder declare
  auto f_rids_vect_fb = floats_builder.CreateVector( rids_vect ) ;
  auto floats_vect_fb = floats_builder.CreateVector( floats_vect ) ;

  Tables::Cols_floatBuilder cols_float_builder( floats_builder ) ;
  cols_float_builder.add_RIDs( f_rids_vect_fb ) ;
  cols_float_builder.add_att0( floats_vect_fb ) ;

  auto cols_float = cols_float_builder.Finish() ;
  floats_builder.Finish( cols_float ) ;

  // save record in bufferlist bl
  const char* floats_fb = reinterpret_cast<char*>( floats_builder.GetBufferPointer() ) ;

  int float_bfsz = floats_builder.GetSize() ;
  librados::bufferlist bl_floats ;
  bl_floats.append( floats_fb, float_bfsz ) ;

  // append to transposed_bl_seq bufferlist
  ::encode( bl_floats, transposed_bl_seq ) ;

  return transposed_bl_seq ;
}

librados::bufferlist set_rows() {

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

  //place these before record_builder declare
  auto table_name    = builder.CreateString( "thisisatable" ) ;
  auto rids_vect_fb  = builder.CreateVector( rids_vect ) ;
  auto int_vect_fb   = builder.CreateVector( int_vect ) ;
  auto float_vect_fb = builder.CreateVector( float_vect ) ;

  Tables::RowsBuilder rows_builder( builder ) ;
  rows_builder.add_table_name( table_name ) ;
  rows_builder.add_RIDs( rids_vect_fb ) ;
  rows_builder.add_att0( int_vect_fb ) ;
  rows_builder.add_att1( float_vect_fb ) ;

  auto rows = rows_builder.Finish() ;
  builder.Finish( rows ) ;

  uint8_t *buffer_pointer = builder.GetBufferPointer() ;
  int size = builder.GetSize() ;
  auto rows_read = Tables::GetRows( buffer_pointer ) ;
  auto rids_read = rows_read->RIDs() ;
  auto att0_read = rows_read->att0() ;
  auto att1_read = rows_read->att1() ;

  std::cout << "size : " << size << std::endl ;
  std::cout << "RIDs : " << rids_read << std::endl ;
  std::cout << rids_read->Length() << std::endl ;
  std::cout << rids_read->Get(0) << std::endl ;
  std::cout << rids_read->Get(1) << std::endl ;
  std::cout << rids_read->Get(2) << std::endl ;

  std::cout << "att0 : " << att0_read << std::endl ;
  std::cout << att0_read->Length() << std::endl ;
  std::cout << att0_read->Get(0) << std::endl ;
  std::cout << att0_read->Get(1) << std::endl ;
  std::cout << att0_read->Get(2) << std::endl ;

  std::cout << "att1 : " << att1_read << std::endl ;
  std::cout << att1_read->Length() << std::endl ;
  std::cout << att1_read->Get(0) << std::endl ;
  std::cout << att1_read->Get(1) << std::endl ;
  std::cout << att1_read->Get(2) << std::endl ;

  const char* fb = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;
  int bufsz      = builder.GetSize() ;
  librados::bufferlist bl ;
  bl.append( fb, bufsz ) ;

  return bl ;

  //bl->append( "qwerty" ) ;
  //bl->append( builder ) ;
}
