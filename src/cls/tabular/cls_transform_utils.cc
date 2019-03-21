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
#include "transforms_generated.h"
#include "include/rados/librados.hpp"

void test() {
  std::cout << "blee" << std::endl ;

  // write to flatbuffer
  librados::bufferlist bl_seq ;

  auto bl0 = record0() ;
  auto bl1 = record1() ;
  auto bl2 = record2() ;
  ::encode( bl0, bl_seq ) ;
  ::encode( bl1, bl_seq ) ;
  ::encode( bl2, bl_seq ) ;

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

  librados::bufferlist bufl ;
  bufl.append( "asdf" ) ;

  // write bl_seq
  const char *obj_name  = "blah" ;
  //ret = ioctx.write_full( obj_name, bufl ) ;
  bufferlist::iterator p = bl_seq.begin();
  size_t i = p.get_remaining() ;
  std::cout << i << std::endl ;
  ret = ioctx.write( obj_name, bl_seq, i, 0 ) ;
  checkret(ret, 0);

  // read bl_seq
  librados::bufferlist wrapped_bl_seq ;
  int num_bytes_read = ioctx.read( obj_name, wrapped_bl_seq, (size_t)0, (uint64_t)0 ) ;

  //ceph::bufferlist::iterator it = bl_seq.begin() ;
  //std::cout << it.get_remaining() << std::endl ;
  ceph::bufferlist::iterator it_wrapped = wrapped_bl_seq.begin() ;
  std::cout << it_wrapped.get_remaining() << std::endl ;

  while( it_wrapped.get_remaining() > 0 ) {
    ceph::bufferlist bl ;
    ::decode( bl, it_wrapped ) ; // this decrements get_remaining by moving iterator

    const char* fb = bl.c_str() ;
    auto root = Tables::GetRecord( fb ) ;

    auto table_name = root->table_name() ;
    auto att0_read = root->att0() ;
    auto att1_read = root->att1() ;
    auto att2_read = root->att2() ;
    auto att3_read = root->att3() ;

    std::cout << "table_name : " << table_name << std::endl ;
    std::cout << "att0 : " << att0_read << std::endl ;
    std::cout << "att1 : " << att1_read << std::endl ;
    std::cout << "att2 : " << att2_read << std::endl ;
    std::cout << "att3 : " << att3_read << std::endl ;
  }

  ioctx.close();
}

librados::bufferlist record0() {
  flatbuffers::FlatBufferBuilder builder(1024) ;
  auto table_name = builder.CreateString(  "thisisatable" ) ; //place this before record_builder declare
  Tables::RecordBuilder record_builder( builder ) ;
  record_builder.add_table_name( table_name ) ;
  record_builder.add_att0( 10 ) ;
  record_builder.add_att1( 11 ) ;
  record_builder.add_att2( 12 ) ;
  record_builder.add_att3( 13 ) ;
  auto arec = record_builder.Finish() ;
  builder.Finish( arec ) ;

  uint8_t *buffer_pointer = builder.GetBufferPointer() ;
  int size = builder.GetSize() ;
  auto arec_read = Tables::GetRecord( buffer_pointer ) ;
  auto att0_read = arec_read->att0() ;
  auto att1_read = arec_read->att1() ;
  auto att2_read = arec_read->att2() ;
  auto att3_read = arec_read->att3() ;

  std::cout << "size : " << size << std::endl ;
  std::cout << "att0 : " << att0_read << std::endl ;
  std::cout << "att1 : " << att1_read << std::endl ;
  std::cout << "att2 : " << att2_read << std::endl ;
  std::cout << "att3 : " << att3_read << std::endl ;

  const char* fb = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;
  int bufsz      = builder.GetSize() ;
  librados::bufferlist bl ;
  bl.append( fb, bufsz ) ;

  return bl ;

  //bl->append( "qwerty" ) ;
  //bl->append( builder ) ;
}

librados::bufferlist record1() {
  flatbuffers::FlatBufferBuilder builder(1024) ;
  auto table_name = builder.CreateString(  "thisisatable" ) ; //place this before record_builder declare
  Tables::RecordBuilder record_builder( builder ) ;
  record_builder.add_table_name( table_name ) ;
  record_builder.add_att0( 20 ) ;
  record_builder.add_att1( 21 ) ;
  record_builder.add_att2( 22 ) ;
  record_builder.add_att3( 23 ) ;
  auto arec = record_builder.Finish() ;
  builder.Finish( arec ) ;

  uint8_t *buffer_pointer = builder.GetBufferPointer() ;
  int size = builder.GetSize() ;
  auto arec_read = Tables::GetRecord( buffer_pointer ) ;
  auto att0_read = arec_read->att0() ;
  auto att1_read = arec_read->att1() ;
  auto att2_read = arec_read->att2() ;
  auto att3_read = arec_read->att3() ;

  std::cout << "size : " << size << std::endl ;
  std::cout << "att0 : " << att0_read << std::endl ;
  std::cout << "att1 : " << att1_read << std::endl ;
  std::cout << "att2 : " << att2_read << std::endl ;
  std::cout << "att3 : " << att3_read << std::endl ;

  const char* fb = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;
  int bufsz      = builder.GetSize() ;
  librados::bufferlist bl ;
  bl.append( fb, bufsz ) ;

  return bl ;
}

librados::bufferlist record2() {
  flatbuffers::FlatBufferBuilder builder(1024) ;
  auto table_name = builder.CreateString(  "thisisatable" ) ; //place this before record_builder declare
  Tables::RecordBuilder record_builder( builder ) ;
  record_builder.add_table_name( table_name ) ;
  record_builder.add_att0( 30 ) ;
  record_builder.add_att1( 31 ) ;
  record_builder.add_att2( 32 ) ;
  record_builder.add_att3( 33 ) ;
  auto arec = record_builder.Finish() ;
  builder.Finish( arec ) ;

  uint8_t *buffer_pointer = builder.GetBufferPointer() ;
  int size = builder.GetSize() ;
  auto arec_read = Tables::GetRecord( buffer_pointer ) ;
  auto att0_read = arec_read->att0() ;
  auto att1_read = arec_read->att1() ;
  auto att2_read = arec_read->att2() ;
  auto att3_read = arec_read->att3() ;

  std::cout << "size : " << size << std::endl ;
  std::cout << "att0 : " << att0_read << std::endl ;
  std::cout << "att1 : " << att1_read << std::endl ;
  std::cout << "att2 : " << att2_read << std::endl ;
  std::cout << "att3 : " << att3_read << std::endl ;

  const char* fb = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;
  int bufsz      = builder.GetSize() ;
  librados::bufferlist bl ;
  bl.append( fb, bufsz ) ;

  return bl ;
}
