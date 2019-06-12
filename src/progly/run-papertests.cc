/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/

#include <chrono>

#include <fstream>
#include <boost/program_options.hpp>

#include "include/rados/librados.hpp"
#include "include/cephfs/libcephfs.h"

#include "cls/tabular/cls_transform_utils.h"
#include "cls/tabular/cls_tabular.h"
#include "cls/tabular/cls_tabular_utils.h"
#include "query_utils.h"

namespace po = boost::program_options ;

void exp0_save_to_obj( std::string, librados::bufferlist ) ;
void exp0_read_obj( std::string ) ;

int main( int argc, char **argv ) {
  std::cout << "in run-papertests..." << std::endl ;

  std::string PATH = "/home/minion/projects/skyhookdb-ceph/src/progly/paper0/" ;

  // --------------- 1 mb --------------- //
  // read raw data
  std::ifstream infile ;
  std::string line ;
  infile.open( PATH + "dataset_obj_v_file_1mb.txt" ) ;
  while( std::getline( infile, line ) ) {
    std::string oid_1mb = "obj_v_file_1mb" ;
    librados::bufferlist bl ;
    ::encode( line, bl ) ;

    // =========================================================== //
    std::cout << "exp : " << oid_1mb << std::endl ;

    // write to object
    exp0_save_to_obj( oid_1mb, bl ) ;
    // read from obj
    exp0_read_obj( oid_1mb ) ;
    // =========================================================== //

    // =========================================================== //
    // write to file
    // https://github.com/ceph/ceph/blob/master/src/test/libcephfs/access.cc
    // https://github.com/ceph/ceph/blob/master/src/test/libcephfs/access.cc#L197
    // https://github.com/ceph/ceph/blob/1aabb186b760e0d710d80c78df9a6b44a83993e4/src/libcephfs.cc
    // https://github.com/ceph/ceph/blob/1aabb186b760e0d710d80c78df9a6b44a83993e4/src/include/cephfs/libcephfs.h

    // read from file
    //
    // =========================================================== //
  }

  // --------------- 10 mb --------------- //
  // read raw data
  std::ifstream infile_10 ;
  std::string line_10 ;
  infile_10.open( PATH + "dataset_obj_v_file_10mb.txt" ) ;
  while( std::getline( infile_10, line_10 ) ) {
    std::string oid_10mb = "obj_v_file_10mb" ;
    librados::bufferlist bl_10 ;
    ::encode( line_10, bl_10 ) ;

    // =========================================================== //
    std::cout << "exp : " << oid_10mb << std::endl ;

    // write to object
    exp0_save_to_obj( oid_10mb, bl_10 ) ;
    // read from obj
    exp0_read_obj( oid_10mb ) ;
    // =========================================================== //

    // =========================================================== //
    // write to file
    // https://github.com/ceph/ceph/blob/master/src/test/libcephfs/access.cc
    // https://github.com/ceph/ceph/blob/master/src/test/libcephfs/access.cc#L197
    // https://github.com/ceph/ceph/blob/1aabb186b760e0d710d80c78df9a6b44a83993e4/src/libcephfs.cc
    // https://github.com/ceph/ceph/blob/1aabb186b760e0d710d80c78df9a6b44a83993e4/src/include/cephfs/libcephfs.h

    // read from file
    //
    // =========================================================== //
  }

  // --------------- 100 mb --------------- //
  std::ifstream infile_100 ;
  std::string line_100 ;
  infile_100.open( PATH + "dataset_obj_v_file_100mb.txt" ) ;
  while( std::getline( infile_100, line_100 ) ) {
    std::string oid_100mb = "obj_v_file_100mb" ;
    librados::bufferlist bl_100 ;
    ::encode( line_100, bl_100 ) ;

    // =========================================================== //
    std::cout << "exp : " << oid_100mb << std::endl ;

    // write to object
    //exp0_save_to_obj( oid_100mb, bl_100 ) ;
    // read from obj
    exp0_read_obj( oid_100mb ) ;
    // =========================================================== //

    // =========================================================== //
    // write to file
    // https://github.com/ceph/ceph/blob/master/src/test/libcephfs/access.cc
    // https://github.com/ceph/ceph/blob/master/src/test/libcephfs/access.cc#L197
    // https://github.com/ceph/ceph/blob/1aabb186b760e0d710d80c78df9a6b44a83993e4/src/libcephfs.cc
    // https://github.com/ceph/ceph/blob/1aabb186b760e0d710d80c78df9a6b44a83993e4/src/include/cephfs/libcephfs.h

    // read from file
    //
    // =========================================================== //
  }

//  // --------------- 1000 mb --------------- //
//  std::ifstream infile_1000 ;
//  std::string line_1000 ;
//  infile_1000.open( PATH + "dataset_obj_v_file_1000mb.txt" ) ;
//  while( std::getline( infile_1000, line_1000 ) ) {
//    std::string oid_1000mb = "obj_v_file_1000mb" ;
//    librados::bufferlist bl_1000 ;
//    ::encode( line_1000, bl_1000 ) ;
//
//    // =========================================================== //
//    std::cout << "exp : " << oid_1000mb << std::endl ;
//
//    // write to object
//    //exp0_save_to_obj( oid_100mb, bl_100 ) ;
//    // read from obj
//    exp0_read_obj( oid_1000mb ) ;
//    // =========================================================== //
//
//    // =========================================================== //
//    // write to file
//    // https://github.com/ceph/ceph/blob/master/src/test/libcephfs/access.cc
//    // https://github.com/ceph/ceph/blob/master/src/test/libcephfs/access.cc#L197
//    // https://github.com/ceph/ceph/blob/1aabb186b760e0d710d80c78df9a6b44a83993e4/src/libcephfs.cc
//    // https://github.com/ceph/ceph/blob/1aabb186b760e0d710d80c78df9a6b44a83993e4/src/include/cephfs/libcephfs.h
//
//    // read from file
//    //
//    // =========================================================== //
//  }

  return 0 ;
}

void exp0_save_to_obj( std::string oid, librados::bufferlist bl ) {

  // save to ceph object
  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  checkret(ret, 0);

  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( "paper_exps" , ioctx ) ;
  checkret( ret, 0 ) ;

  // write bl_seq to ceph object
  const char *obj_name = oid.c_str() ;
  bufferlist::iterator p = bl.begin();
  size_t num_bytes_written = p.get_remaining() ;
  std::cout << "num_bytes_written : " << num_bytes_written << std::endl ;
  ret = ioctx.write( obj_name, bl, num_bytes_written, 0 ) ;
  checkret(ret, 0);

  ioctx.close() ;
}

void exp0_read_obj( std::string oid ) {
  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  checkret(ret, 0);

  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( "paper_exps" , ioctx ) ;
  checkret( ret, 0 ) ;

  // write bl_seq to ceph object
  librados::bufferlist bl ;
  auto start = std::chrono::high_resolution_clock::now();
  int num_bytes_read = ioctx.read( oid.c_str(), bl, (size_t)0, (uint64_t)0 ) ;
  auto stop  = std::chrono::high_resolution_clock::now();
  std::cout << "num_bytes_read : " << num_bytes_read << std::endl ;
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "duration : " << duration << std::endl ;

  ioctx.close() ;
}
