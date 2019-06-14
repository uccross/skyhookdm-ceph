/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/

/*
bin/run-papertests /home/minion/projects/skyhookdb-ceph/src/progly/paper0/dataset_case1_1mb.txt /home/minion/projects/skyhookdb-ceph/src/progly/paper0/res_test.txt paper_exps 0 test0
bin/run-papertests /home/minion/projects/skyhookdb-ceph/src/progly/paper0/dataset_newline_1mb.txt /home/minion/projects/skyhookdb-ceph/src/progly/paper0/res_test.txt paper_exps 1 test1
*/

#include <iostream>
#include <ctime>

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

void raw_whole_write( std::string, std::string, std::string ) ;
void raw_iter_write( std::string, std::string, std::string ) ;
void flatbuffer_write_rows( std::string, std::string, std::string ) ;
void flatbuffer_write_colint( std::string, std::string, std::string ) ;
std::string read_obj( std::string ) ;

int main( int argc, char **argv ) {
  std::cout << "in run-papertests..." << std::endl ;

  // datetime code from https://stackoverflow.com/a/16358264
  time_t rawtime ;
  struct tm * timeinfo ;
  char buffer[80] ;
  time ( &rawtime ) ;
  timeinfo = localtime( &rawtime ) ;
  strftime( buffer, sizeof( buffer ),"%Y%m%d_%H%M%S", timeinfo ) ;
  std::string str( buffer ) ;

  //std::string res_filename = "results_" + str + ".txt" ;
  //std::string PATH     = "/home/minion/projects/skyhookdb-ceph/src/progly/paper0/" ;
  //std::string RES_PATH = "/home/minion/projects/skyhookdb-ceph/src/progly/paper0/" + res_filename ;

  std::string DATA_PATH    = argv[1] ;
  std::string RES_PATH     = argv[2] ;
  std::string poolname     = argv[3] ;
  int write_type           = std::atoi( argv[4] ) ;
  std::string oidname      = argv[5] ;

  std::ofstream res_file ;
  res_file.open( RES_PATH, ios::out | ios::app ) ;

  std::cout << "================================" << std::endl ;
  std::cout << "running with params:" << std::endl ;
  std::cout << "DATA_PATH    = " << DATA_PATH << std::endl ;
  std::cout << "RES_PATH     = " << RES_PATH << std::endl ;
  std::cout << "poolname     = " << poolname << std::endl ;
  std::cout << "write_type   = " << write_type << std::endl ;
  std::cout << "oidname      = " << oidname << std::endl ;

  res_file << "================================" << std::endl ;
  res_file << "running with params:" << std::endl ;
  res_file << "DATA_PATH    = " << DATA_PATH << std::endl ;
  res_file << "RES_PATH     = " << RES_PATH << std::endl ;
  res_file << "poolname     = " << poolname << std::endl ;
  res_file << "write_type   = " << write_type << std::endl ;
  res_file << "oidname      = " << oidname << std::endl ;

  std::cout << "--->exp : " << oidname << std::endl ;
  if( write_type == 0 ) {
    raw_whole_write( oidname, DATA_PATH, poolname ) ;
  }
  else if( write_type == 1 ) {
    raw_whole_write( oidname, DATA_PATH, poolname ) ;
  }
  else if( write_type == 2 ) {
    flatbuffer_write_rows( oidname, DATA_PATH, poolname ) ;
  }
  else if( write_type == 3 ) {
    flatbuffer_write_rows( oidname, DATA_PATH, poolname ) ;
  }
  else {
    std::cout << "unrecognized write_type '" << std::to_string( write_type ) << "'" << std::endl ;
  }
  std::string res = read_obj( oidname ) ;
  res_file << res << "\n" ;
  res_file.close() ;

  return 0 ;
}

void raw_whole_write( std::string oid, std::string filename, std::string poolname ) {

  std::ifstream infile ;
  infile.open( filename ) ;

  std::string line ;
  librados::bufferlist bl ;
  while( std::getline( infile, line ) ) {
    ::encode( line, bl ) ;
  }

  // save to ceph object
  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  checkret(ret, 0);

  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( poolname.c_str(), ioctx ) ;
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

void raw_iter_write( std::string oid, std::string filename, std::string poolname ) {

  std::ifstream infile ;
  infile.open( filename ) ;

  std::string line ;
  librados::bufferlist bl ;
  while( std::getline( infile, line ) ) {
    //line.erase( std::remove( line.begin(), line.end(), '\n' ), line.end() ) ;
    ::encode( line, bl ) ;
  }

  // save to ceph object
  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  checkret(ret, 0);

  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( poolname.c_str(), ioctx ) ;
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

void flatbuffer_write_colint( std::string oid, std::string filename, std::string poolname ) {

  flatbuffers::FlatBufferBuilder builder( 1024 ) ;
  librados::bufferlist bl_seq ;

  // build fb meta bufferlist
  std::vector< uint8_t > meta_schema ;
  meta_schema.push_back( (uint8_t)0 ) ; // 0 --> uint64
  auto a = builder.CreateVector( meta_schema ) ;

  std::vector< uint64_t > rids_vect ;
  std::vector< uint64_t > int_vect ;

  // build fb data bufferlists
  std::ifstream infile ;
  infile.open( filename ) ;

  std::string line ;
  int count = 1 ;
  while( std::getline( infile, line ) ) {
    rids_vect.push_back( count ) ;
    int_vect.push_back( std::atoi( line.c_str() ) ) ;
    count++ ;
  }

  //place these before record_builder declare
  auto col_name     = builder.CreateString( "att0" ) ;
  auto RIDs         = builder.CreateVector( rids_vect ) ;
  auto int_vect_fb  = builder.CreateVector( int_vect ) ;
  uint8_t col_index = 0 ;
  uint8_t nrows     = int_vect.size() ;

  // create the Rows flatbuffer:
  auto col = Tables::CreateColInt(
    builder,
    0,
    0,
    col_name,
    col_index,
    nrows,
    RIDs,
    int_vect_fb ) ;

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
  ret = cluster.ioctx_create( poolname.c_str(), ioctx ) ;
  checkret( ret, 0 ) ;

  // write bl_seq to ceph object
  const char *obj_name = oid.c_str() ;
  bufferlist::iterator p = bl.begin();
  size_t num_bytes_written = p.get_remaining() ;
  std::cout << "num_bytes_written : " << num_bytes_written << std::endl ;
  ret = ioctx.write( obj_name, bl_seq, num_bytes_written, 0 ) ;
  checkret(ret, 0);

  ioctx.close() ;
}

void flatbuffer_write_rows( std::string oid, std::string filename, std::string poolname ) {

  flatbuffers::FlatBufferBuilder builder( 1024 ) ;
  librados::bufferlist bl_seq ;

  // build fb meta bufferlist
  std::vector< uint8_t > meta_schema ;
  meta_schema.push_back( (uint8_t)0 ) ; // 0 --> uint64
  auto a = builder.CreateVector( meta_schema ) ;

  std::vector< uint64_t > rids_vect ;
  std::vector< uint64_t > int_vect ;

  // build fb data bufferlists
  std::ifstream infile ;
  infile.open( filename ) ;

  std::string line ;
  int count = 1 ;
  while( std::getline( infile, line ) ) {
    rids_vect.push_back( count ) ;
    int_vect.push_back( std::atoi( line.c_str() ) ) ;
    count++ ;
  }

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
  ret = cluster.ioctx_create( poolname.c_str(), ioctx ) ;
  checkret( ret, 0 ) ;

  // write bl_seq to ceph object
  const char *obj_name = oid.c_str() ;
  bufferlist::iterator p = bl.begin();
  size_t num_bytes_written = p.get_remaining() ;
  std::cout << "num_bytes_written : " << num_bytes_written << std::endl ;
  ret = ioctx.write( obj_name, bl_seq, num_bytes_written, 0 ) ;
  checkret(ret, 0);

  ioctx.close() ;
}

std::string read_obj( std::string oid ) {

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

  std::ostringstream duration_str ;
  duration_str << duration ;

  ioctx.close() ;

  std::string out_str = "" ;
  out_str = out_str + "--->exp : "        + oid + "\n" ;
  out_str = out_str + "num_bytes_read : " + std::to_string( num_bytes_read ) + "\n" ;
  out_str = out_str + "duration : "       + duration_str.str() + "\n" ;

  return out_str ;
}
