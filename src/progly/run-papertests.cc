/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
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

void raw_whole_write( std::string, std::string ) ;
void raw_iter_write( std::string, std::string ) ;
void flatbuffer_write_rows( std::string, std::string ) ;
void flatbuffer_write_colint( std::string, std::string ) ;
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

  std::string res_filename = "results_" + str + ".txt" ;
  std::string PATH     = "/home/minion/projects/skyhookdb-ceph/src/progly/paper0/" ;
  std::string RES_PATH = "/home/minion/projects/skyhookdb-ceph/src/progly/paper0/" + res_filename ;

  std::ofstream res_file ;
  res_file.open( RES_PATH ) ;

  // --------------- 1 mb --------------- //
  std::string oid_1mb = "raw_whole_1mb" ;
  std::string oid_1mb_filename = PATH + "dataset_case1_1mb.txt" ;
  std::cout << "--->exp : " << oid_1mb << std::endl ;
  raw_whole_write( oid_1mb, oid_1mb_filename ) ;
  std::string res_raw_whole_1mb = read_obj( oid_1mb ) ;
  res_file << res_raw_whole_1mb << "\n" ;

  std::string oid_1mb_newline = "raw_iter_1mb" ;
  std::string oid_1mb_newline_filename = PATH + "dataset_newline_1mb.txt" ;
  std::cout << "--->exp : " << oid_1mb_newline << std::endl ;
  raw_whole_write( oid_1mb_newline, oid_1mb_newline_filename ) ;
  std::string res_raw_iter_1mb = read_obj( oid_1mb_newline ) ;
  res_file << res_raw_iter_1mb << "\n" ;

  std::string oid_1mb_flatbuffer_rows = "flatbuffer_rows_1mb" ;
  std::string oid_1mb_flatbuffer_rows_filename = PATH + "dataset_newline_1mb.txt" ;
  std::cout << "--->exp : " << oid_1mb_flatbuffer_rows << std::endl ;
  flatbuffer_write_rows( oid_1mb_flatbuffer_rows, oid_1mb_flatbuffer_rows_filename ) ;
  std::string res_flatbuffer_rows_1mb = read_obj( oid_1mb_flatbuffer_rows ) ;
  res_file << res_flatbuffer_rows_1mb << "\n" ;

  std::string oid_1mb_flatbuffer_colint = "flatbuffer_colint_1mb" ;
  std::string oid_1mb_flatbuffer_colint_filename = PATH + "dataset_newline_1mb.txt" ;
  std::cout << "--->exp : " << oid_1mb_flatbuffer_colint << std::endl ;
  flatbuffer_write_rows( oid_1mb_flatbuffer_colint, oid_1mb_flatbuffer_colint_filename ) ;
  std::string res_flatbuffer_colint_1mb = read_obj( oid_1mb_flatbuffer_colint ) ;
  res_file << res_flatbuffer_colint_1mb << "\n" ;

  // --------------- 10 mb --------------- //
  std::string oid_10mb = "raw_whole_10mb" ;
  std::string oid_10mb_filename = PATH + "dataset_case1_10mb.txt" ;
  raw_whole_write( oid_10mb, oid_10mb_filename ) ;
  std::string res_raw_whole_10mb = read_obj( oid_10mb ) ;
  res_file << res_raw_whole_10mb << "\n" ;

  std::string oid_10mb_newline = "raw_iter_10mb" ;
  std::string oid_10mb_newline_filename = PATH + "dataset_newline_10mb.txt" ;
  std::cout << "--->exp : " << oid_10mb_newline << std::endl ;
  raw_whole_write( oid_10mb_newline, oid_10mb_newline_filename ) ;
  std::string res_raw_iter_10mb = read_obj( oid_10mb_newline ) ;
  res_file << res_raw_iter_10mb << "\n" ;

  std::string oid_10mb_flatbuffer_rows = "flatbuffer_rows_10mb" ;
  std::string oid_10mb_flatbuffer_rows_filename = PATH + "dataset_newline_10mb.txt" ;
  std::cout << "--->exp : " << oid_10mb_flatbuffer_rows << std::endl ;
  flatbuffer_write_rows( oid_10mb_flatbuffer_rows, oid_10mb_flatbuffer_rows_filename ) ;
  std::string res_flatbuffer_rows_10mb = read_obj( oid_10mb_flatbuffer_rows ) ;
  res_file << res_flatbuffer_rows_10mb << "\n" ;

  std::string oid_10mb_flatbuffer_colint = "flatbuffer_colint_10mb" ;
  std::string oid_10mb_flatbuffer_colint_filename = PATH + "dataset_newline_10mb.txt" ;
  std::cout << "--->exp : " << oid_10mb_flatbuffer_colint << std::endl ;
  flatbuffer_write_rows( oid_10mb_flatbuffer_colint, oid_10mb_flatbuffer_colint_filename ) ;
  std::string res_flatbuffer_colint_10mb = read_obj( oid_10mb_flatbuffer_colint ) ;
  res_file << res_flatbuffer_colint_10mb << "\n" ;

  res_file.close() ;

  return 0 ;
}

void raw_whole_write( std::string oid, std::string filename ) {

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

void raw_iter_write( std::string oid, std::string filename ) {

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

void flatbuffer_write_colint( std::string oid, std::string filename ) {

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
  ret = cluster.ioctx_create( "paper_exps" , ioctx ) ;
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

void flatbuffer_write_rows( std::string oid, std::string filename ) {

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
  ret = cluster.ioctx_create( "paper_exps" , ioctx ) ;
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
