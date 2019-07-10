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
bin/run-papertests /home/minion/projects/skyhookdb-ceph/src/progly/katfiles/dataset_case1_1mb.txt /home/minion/res_test.txt paper_exps 0 test0 sum
bin/run-papertests /home/minion/projects/skyhookdb-ceph/src/progly/katfiles/dataset_newline_1mb.txt /home/minion/res_test.txt paper_exps 1 test1 randomsearch
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

void raw_whole_write( std::string, std::string, std::string, bool ) ;
void raw_iter_write( std::string, std::string, std::string, bool ) ;
std::string read_obj( std::string, std::string, int, bool ) ;

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
  std::string optype       = argv[6] ;
  bool debug               = true ;

  std::ofstream res_file ;
  res_file.open( RES_PATH, std::ios::out | std::ios::app ) ;

  if( debug ) {
    std::cout << "================================" << std::endl ;
    std::cout << "running with params:" << std::endl ;
    std::cout << "DATA_PATH    = " << DATA_PATH << std::endl ;
    std::cout << "RES_PATH     = " << RES_PATH << std::endl ;
    std::cout << "poolname     = " << poolname << std::endl ;
    std::cout << "write_type   = " << write_type << std::endl ;
    std::cout << "oidname      = " << oidname << std::endl ;
    std::cout << "timestamp    = " << str << std::endl ;
    std::cout << "--->exp : " << oidname << std::endl ;
  }

  if( write_type == 0 ) {
    raw_whole_write( oidname, DATA_PATH, poolname, debug ) ;
  }
  else if( write_type == 1 ) {
    raw_whole_write( oidname, DATA_PATH, poolname, debug ) ;
  }
  else {
    std::cout << "unrecognized write_type '" << std::to_string( write_type ) << "'" << std::endl ;
  }
  std::string res = read_obj( oidname, optype, write_type, debug ) ;
  res_file << res << "\n" ;
  res_file.close() ;

  return 0 ;
}

void raw_whole_write( std::string oid, std::string filename, std::string poolname, bool debug ) {

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

  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( poolname.c_str(), ioctx ) ;

  // write bl_seq to ceph object
  const char *obj_name = oid.c_str() ;
  bufferlist::iterator p = bl.begin();
  size_t num_bytes_written = p.get_remaining() ;
  if( debug )
    std::cout << "num_bytes_written : " << num_bytes_written << std::endl ;
  ret = ioctx.write( obj_name, bl, num_bytes_written, 0 ) ;

  ioctx.close() ;
}

void raw_iter_write( std::string oid, std::string filename, std::string poolname, bool debug ) {

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

  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( poolname.c_str(), ioctx ) ;

  // write bl_seq to ceph object
  const char *obj_name = oid.c_str() ;
  bufferlist::iterator p = bl.begin();
  size_t num_bytes_written = p.get_remaining() ;
  if( debug )
    std::cout << "num_bytes_written : " << num_bytes_written << std::endl ;
  ret = ioctx.write( obj_name, bl, num_bytes_written, 0 ) ;

  ioctx.close() ;
}

void do_sum( librados::bufferlist bl_seq, int write_type, bool debug ) {

  std::vector< uint64_t > these_ints ;

  // raw
  if( write_type == 0 ) {
    ceph::bufferlist::iterator it_bl_seq = bl_seq.begin() ;
    ceph::bufferlist bl ;
    ::decode( bl, it_bl_seq ) ;
    const char* raw = bl.c_str() ;
    int count = 0 ;
    std::string tmp = "" ;

    for( int i = 0; i < std::strlen( raw ); i++ ) {
      tmp = tmp + raw[i] ;
      count++ ;
      if( count == 0 ) {
        continue ;
      }
      else if( count % 6 == 0 ) {
        if( debug )
          std::cout << "appending tmp = " << tmp << std::endl ;
        these_ints.push_back( std::stoi( tmp ) ) ;
        count = 0 ;
        tmp = "" ;
      }
    }
  }
  // raw indiv
  else if( write_type == 1 ) {
    ceph::bufferlist::iterator it_bl_seq = bl_seq.begin() ;
    while( it_bl_seq.get_remaining() > 0 ) {
      ceph::bufferlist bl ;
      ::decode( bl, it_bl_seq ) ;
      const char* raw = bl.c_str() ;
      if( debug )
        std::cout << "appending raw = " << raw << std::endl ;
      these_ints.push_back( std::stoi( raw ) ) ;
    }
  }
  else {
    std::cout << ">> unrecognized write_type '" << write_type  << "'" << std::endl ;
  }

  // perform sum
  uint64_t the_sum = 0 ;
  for( int i = 0; i < these_ints.size(); i++ ) {
    the_sum = the_sum + these_ints[i] ;
  }
  std::cout << "sum : " << std::to_string( the_sum ) << std::endl ;
  std::cout << "num ints : " << std::to_string( these_ints.size() ) << std::endl ;
}

void do_randomsearch( librados::bufferlist bl_seq, int write_type, bool debug ) {
  int search_for = 551851 ;
  bool found_it = false ;
  
  // raw
  if( write_type == 0 ) {
    ceph::bufferlist::iterator it_bl_seq = bl_seq.begin() ;
    ceph::bufferlist bl ;
    ::decode( bl, it_bl_seq ) ;
    const char* raw = bl.c_str() ;
    int count = 0 ;
    std::string tmp = "" ;

    for( int i = 0; i < std::strlen( raw ); i++ ) {
      tmp = tmp + raw[i] ;
      count++ ;
      if( count == 0 ) {
        continue ;
      }
      else if( count % 6 == 0 ) {
        if( debug )
          std::cout << "checking tmp = " << tmp << std::endl ;
        if( std::stoi( tmp ) == search_for )
          found_it = true ;
        count = 0 ;
        tmp = "" ;
      }
    }
  }
  // raw indiv
  else if( write_type == 1 ) {
    ceph::bufferlist::iterator it_bl_seq = bl_seq.begin() ;
    while( it_bl_seq.get_remaining() > 0 ) {
      ceph::bufferlist bl ;
      ::decode( bl, it_bl_seq ) ;
      const char* raw = bl.c_str() ;
      if( debug )
        std::cout << "checking raw = " << raw << std::endl ;
      if( std::stoi( raw ) == search_for )
        found_it = true ;
    }
  }
  else {
    std::cout << ">> unrecognized write_type '" << write_type  << "'" << std::endl ;
  }

  // perform sum
  std::cout << "search_for : " << std::to_string( search_for ) << std::endl ;
  std::cout << "found_it   : " << std::to_string( found_it ) << std::endl ;
}

std::string read_obj( std::string oid, std::string optype, int write_type, bool debug ) {

  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();

  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( "paper_exps" , ioctx ) ;

  // write bl_seq to ceph object
  librados::bufferlist bl_seq ;
  auto start = std::chrono::high_resolution_clock::now() ;
  int num_bytes_read = ioctx.read( oid.c_str(), bl_seq, (size_t)0, (uint64_t)0 ) ;
  auto stop  = std::chrono::high_resolution_clock::now() ;
  if( debug )
    std::cout << "num_bytes_read : " << num_bytes_read << std::endl ;
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start) ;
  if( debug )
    std::cout << "duration : " << duration << std::endl ;

  // do sum operation with results
  if( optype == "sum" )
    do_sum( bl_seq, write_type, debug ) ;
  // or, do random search operation with results
  else if( optype == "randomsearch" )
    do_randomsearch( bl_seq, write_type, debug ) ;
  else
    std::cout << ">> unrecognized optype '" << optype << "'" << std::endl ;

  std::ostringstream duration_str ;
  duration_str << duration ;

  ioctx.close() ;

  std::string out_str = "" ;
  out_str = out_str + "--->exp : "        + oid + "\n" ;
  out_str = out_str + "num_bytes_read : " + std::to_string( num_bytes_read ) + "\n" ;
  out_str = out_str + "duration : "       + duration_str.str() + "\n" ;

  if( debug )
    std::cout << out_str << std::endl ;

  return out_str ;
}
