/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/

// std libs
#include <fstream>
#include <boost/program_options.hpp>
#include <iostream>
#include <algorithm>
#include <atomic>
#include <thread>
#include <condition_variable>
#include <chrono>
#include "re2/re2.h"

// ceph includes
#include "include/rados/librados.hpp"

// other includes
#include "cls/split/cls_split.h"

namespace po = boost::program_options;

int main(int argc, char **argv)
{
  std::cout << "starting splits..." << std::endl ;

  // split_12
  librados::bufferlist id1_blist_in ;
  librados::bufferlist id1_blist_out0 ;
  librados::bufferlist id1_blist_out1 ;

  std::string str_split_12 = "teststr" ;

  std::vector<std::pair<std::string, int>> data_split_12 ;
  dataset ds_split_12 ;

  for( unsigned int i = 0; i < str_split_12.length(); i++ ) {
    std::string astr ;
    auto achar = str_split_12.c_str()[ i ] ;
    astr.push_back( achar ) ;
    ds_split_12.data.push_back( std::make_pair( astr, 1 ) ) ;
  }

  std::vector< dataset > table_split_12 ;

  table_split_12.push_back( ds_split_12 ) ;

  ::encode( table_split_12, id1_blist_in ) ;
  ceph_split( &id1_blist_in, &id1_blist_out0, &id1_blist_out1, 1 ) ;

  std::vector< dataset > decoded_id1_blist_in ;
  librados::bufferlist::iterator decoded_id1_biter_in( &id1_blist_in ) ;
  ::decode( decoded_id1_blist_in, decoded_id1_biter_in ) ;
  std::vector< dataset > decoded_id1_blist_out0 ;
  librados::bufferlist::iterator decoded_id1_biter_out0( &id1_blist_out0 ) ;
  ::decode( decoded_id1_blist_out0, decoded_id1_biter_out0 ) ;
  std::vector< dataset > decoded_id1_blist_out1 ;
  librados::bufferlist::iterator decoded_id1_biter_out1( &id1_blist_out1 ) ;
  ::decode( decoded_id1_blist_out1, decoded_id1_biter_out1 ) ;

  std::cout << decoded_id1_blist_in[0].toString() << std::endl ;
  std::cout << decoded_id1_blist_out0[0].toString() << std::endl ;
  std::cout << decoded_id1_blist_out1[0].toString() << std::endl ;

  assert( decoded_id1_blist_in[0].toString()   == "dataset :  t, 1  e, 1  s, 1  t, 1  s, 1  t, 1  r, 1" ) ;
  assert( decoded_id1_blist_out0[0].toString() == "dataset :  t, 1  e, 1  s, 1" ) ;
  assert( decoded_id1_blist_out1[0].toString() == "dataset :  t, 1  s, 1  t, 1  r, 1" ) ;

  // split_pattern
  librados::bufferlist id2_blist_in ;
  librados::bufferlist id2_blist_out0 ;
  librados::bufferlist id2_blist_out1 ;

  std::string str_split_pattern = "teststr" ;

  std::vector<std::pair<std::string, int>> data_split_pattern ;
  dataset ds_split_pattern ;

  for( unsigned int i = 0; i < str_split_pattern.length(); i++ ) {
    std::string astr ;
    auto achar = str_split_pattern.c_str()[ i ] ;
    astr.push_back( achar ) ;
    ds_split_pattern.data.push_back( std::make_pair( astr, 1 ) ) ;
  }

  std::vector< dataset > table_split_pattern ;

  table_split_pattern.push_back( ds_split_pattern ) ;

  ::encode( table_split_pattern, id2_blist_in ) ;
  ceph_split( &id2_blist_in, &id2_blist_out0, &id2_blist_out1, 2, "s" ) ;

  std::vector< dataset > decoded_id2_blist_in ;
  librados::bufferlist::iterator decoded_id2_biter_in( &id2_blist_in ) ;
  ::decode( decoded_id2_blist_in, decoded_id2_biter_in ) ;
  std::vector< dataset > decoded_id2_blist_out0 ;
  librados::bufferlist::iterator decoded_id2_biter_out0( &id2_blist_out0 ) ;
  ::decode( decoded_id2_blist_out0, decoded_id2_biter_out0 ) ;
  std::vector< dataset > decoded_id2_blist_out1 ;
  librados::bufferlist::iterator decoded_id2_biter_out1( &id2_blist_out1 ) ;
  ::decode( decoded_id2_blist_out1, decoded_id2_biter_out1 ) ;

  //std::cout << decoded_id2_blist_in[0].toString() << std::endl ;
  //std::cout << decoded_id2_blist_out0[0].toString() << std::endl ;
  //std::cout << decoded_id2_blist_out1[0].toString() << std::endl ;

  assert( decoded_id2_blist_in[0].toString()   == "dataset :  t, 1  e, 1  s, 1  t, 1  s, 1  t, 1  r, 1" ) ;
  assert( decoded_id2_blist_out0[0].toString() == "dataset :  t, 1  e, 1" ) ;
  assert( decoded_id2_blist_out1[0].toString() == "dataset :  s, 1  t, 1  s, 1  t, 1  r, 1" ) ;

  // split_noop
  librados::bufferlist id0_blist_in ;
  librados::bufferlist id0_blist_out0 ;
  librados::bufferlist id0_blist_out1 ;

  std::string str_split_noop = "teststr" ;

  std::vector<std::pair<std::string, int>> data_split_noop ;
  dataset ds_split_noop ;

  for( unsigned int i = 0; i < str_split_noop.length(); i++ ) {
    std::string astr ;
    auto achar = str_split_noop.c_str()[ i ] ;
    astr.push_back( achar ) ;
    ds_split_noop.data.push_back( std::make_pair( astr, 1 ) ) ;
  }

  std::vector< dataset > table_split_noop ;

  table_split_noop.push_back( ds_split_noop ) ;

  ::encode( table_split_noop, id0_blist_in ) ;
  ceph_split( &id0_blist_in, &id0_blist_out0, &id0_blist_out1, 0 ) ;

  std::cout << "...splits done. phew!" << std::endl ;
  return 0 ;
}
