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
bin/write-papertests ../src/progly/katfiles/dataset_arity1_1000000_rows-0.txt paper_exps 0 concat-data
bin/write-papertests ../src/progly/katfiles/dataset_arity1_1000000_rows-0.txt paper_exps 1 separate-data
*/

#include <iostream>
#include <ctime>
#include <chrono>
#include <fstream>
#include <boost/program_options.hpp>

#include "include/rados/librados.hpp"
#include "include/cephfs/libcephfs.h"

#include <string>
#include <sstream>

#include "include/types.h"
#include <errno.h>
#include <time.h>

#include "objclass/objclass.h"

namespace po = boost::program_options ;

void raw_concat_write( std::string, std::string, std::string, bool ) ;
void raw_separate_write( std::string, std::string, std::string, bool ) ;

int main( int argc, char **argv ) {
  std::cout << "in write-papertests..." << std::endl ;

  // datetime code from https://stackoverflow.com/a/16358264
  time_t rawtime ;
  struct tm * timeinfo ;
  char buffer[80] ;
  time ( &rawtime ) ;
  timeinfo = localtime( &rawtime ) ;
  strftime( buffer, sizeof( buffer ),"%Y%m%d_%H%M%S", timeinfo ) ;
  std::string str( buffer ) ;

  std::string DATA_PATH    = argv[1] ;
  std::string poolname     = argv[2] ;
  int write_type           = std::atoi( argv[3] ) ;
  std::string oidname      = argv[4] ;
  bool debug               = true ;

  std::ofstream res_file ;

  if( debug ) {
    std::cout << "================================" << std::endl ;
    std::cout << "running with params:" << std::endl ;
    std::cout << "DATA_PATH    = " << DATA_PATH << std::endl ;
    std::cout << "poolname     = " << poolname << std::endl ;
    std::cout << "write_type   = " << write_type << std::endl ;
    std::cout << "oidname      = " << oidname << std::endl ;
    std::cout << "timestamp    = " << str << std::endl ;
    std::cout << "--->exp : " << oidname << std::endl ;
  }

  if( write_type == 0 ) {
    raw_concat_write( oidname, DATA_PATH, poolname, debug ) ;
  }
  else if( write_type == 1 ) {
    raw_separate_write( oidname, DATA_PATH, poolname, debug ) ;
  }
  else {
    std::cout << "unrecognized write_type '" << std::to_string( write_type ) << "'" << std::endl ;
  }

  return 0 ;
}

void raw_concat_write( std::string oid, std::string filename, std::string poolname, bool debug ) {

  std::ifstream infile ;
  infile.open( filename ) ;

  std::string concat_line ;
  std::string line ;
  librados::bufferlist bl ;
  while( std::getline( infile, line ) ) {
    concat_line += line ;
  }
  ::encode( concat_line, bl ) ;

  // save to ceph object
  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();

  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( poolname.c_str(), ioctx ) ;

  // write bl_seq to ceph object
  const char *obj_name = oid.c_str() ;
  bufferlist::iterator p = bl.begin();
  size_t num_bytes_written = p.get_remaining() ;
  if( debug )
    std::cout << "num_bytes_written0 : " << num_bytes_written << std::endl ;
  ret = ioctx.write( obj_name, bl, num_bytes_written, 0 ) ;

  ioctx.close() ;
}

void raw_separate_write( std::string oid, std::string filename, std::string poolname, bool debug ) {

  std::ifstream infile ;
  infile.open( filename ) ;
  std::string line ;
  librados::bufferlist bl ;
  while( std::getline( infile, line ) ) {
    uint32_t this_int = std::stoi(line) ;
    ::encode( this_int, bl ) ;
  }

  // save to ceph object
  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();

  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( poolname.c_str(), ioctx ) ;

  // write bl_seq to ceph object
  const char *obj_name = oid.c_str() ;
  bufferlist::iterator p = bl.begin();
  size_t num_bytes_written = p.get_remaining() ;
  if( debug )
    std::cout << "num_bytes_written1 : " << num_bytes_written << std::endl ;
  ret = ioctx.write( obj_name, bl, num_bytes_written, 0 ) ;

  ioctx.close() ;
}
