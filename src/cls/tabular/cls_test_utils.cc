/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/

#include "cls_test_utils.h"
#include "include/rados/librados.hpp"

void print_test() {
  std::cout << "blah" << std::endl ; 
}

void save_to_basic_fb( std::vector< uint64_t > int_vect ) {
  std::cout << "in save_to_basic_fb..." << std::endl ;

  flatbuffers::FlatBufferBuilder builder( 1024 ) ;
  auto int_vect_fb = builder.CreateVector( int_vect ) ;
  Tables::BasicBuilder basic_builder( builder ) ;
  basic_builder.add_data( int_vect_fb ) ;
  auto res = basic_builder.Finish() ;
  builder.Finish( res ) ;

  const char* fb = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;
  int bufsz      = builder.GetSize() ;
  librados::bufferlist bl ;
  bl.append( fb, bufsz ) ;

  // save to ceph object
  // connect to rados
  librados::Rados cluster ;
  cluster.init(NULL) ;
  cluster.conf_read_file(NULL) ;
  int ret = cluster.connect() ;
  checkret(ret, 0) ;

  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( "tpchflatbuf" , ioctx ) ;
  checkret( ret, 0 ) ;

  // write bl to ceph object
  std::string oid = "test_fbvfile_" + std::to_string( int_vect.size() ) ;
  const char *obj_name = oid.c_str() ;
  bufferlist::iterator p = bl.begin();
  size_t i = p.get_remaining() ;
  std::cout << "num bytes written : " << i << std::endl ;
  ret = ioctx.write( obj_name, bl, i, 0 ) ;
  checkret(ret, 0);

  ioctx.close() ;
}

void read_basic_fb( std::string obj_name ) {
  std::cout << "in read_basic_fb..." << std::endl ;

  // connect to rados
  librados::Rados cluster ;
  cluster.init( NULL ) ;
  cluster.conf_read_file( NULL ) ;
  int ret = cluster.connect() ;
  checkret( ret, 0 ) ;

  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( "tpchflatbuf", ioctx ) ;
  checkret( ret, 0 ) ;

  // read bl_seq
  librados::bufferlist bl ;
  int num_bytes_read = ioctx.read( obj_name.c_str(), bl, (size_t)0, (uint64_t)0 ) ;
  std::cout << "num_bytes_read : " << num_bytes_read << std::endl ;
  const char* fb = bl.c_str() ;
  auto basic_fb  = Tables::GetBasic( fb ) ;
  auto data_fb   = basic_fb->data() ;
  std::vector< uint64_t > data ;
  for( unsigned int i = 0; i < data_fb->Length(); i++ ) {
    data.push_back( (uint64_t)data_fb->Get(i) ) ;
  }
  std::cout << "data:" << std::endl ;
  std::cout << data << std::endl ;
}
