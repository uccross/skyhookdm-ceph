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

// skyhook includes
#include "cls/transform/cls_transform.h"
//#include "query.h"

// #define NDEBUG // uncomment to disable assert()
#include <cassert>

namespace po = boost::program_options;

int main(int argc, char **argv)
{
  std::cout << "starting ceph_transforms..." << std::endl ;

  // ----------------------------------------------------------------------------- //
  // ceph_transform_all

  librados::bufferlist id1_blist_in ;
  librados::bufferlist id1_blist_out ;

  std::string str_all = "teststr" ;

  std::vector<std::pair<std::string, int>> data_all ;
  dataset_transform ds_all ;

  for( unsigned int i = 0; i < str_all.length(); i++ ) {
    std::string astr ;
    auto achar = str_all.c_str()[ i ] ;
    astr.push_back( achar ) ;
    ds_all.data.push_back( std::make_pair( astr, 1 ) ) ;
  } 

  std::vector< dataset_transform > table_all ;

  table_all.push_back( ds_all ) ;

  ::encode( table_all, id1_blist_in ) ;
  ceph_transform( &id1_blist_in, &id1_blist_out, 1 ) ;

  std::vector< dataset_transform > decoded_id1_blist_in ;
  librados::bufferlist::iterator decoded_id1_biter_in( &id1_blist_in ) ;
  ::decode( decoded_id1_blist_in, decoded_id1_biter_in ) ;
  std::vector< dataset_transform > decoded_id1_blist_out ;
  librados::bufferlist::iterator decoded_id1_biter_out( &id1_blist_out ) ;
  ::decode( decoded_id1_blist_out, decoded_id1_biter_out ) ;

  assert( decoded_id1_blist_in[0].toString()  == "dataset_transform :  t, 1  e, 1  s, 1  t, 1  s, 1  t, 1  r, 1" ) ;
  assert( decoded_id1_blist_out[0].toString() == "dataset_transform :  t, 1  e, 1  s, 1  t, 1  s, 1  t, 1  r, 1" ) ;

  // ----------------------------------------------------------------------------- //
  // ceph_transform_reverse

  librados::bufferlist id2_blist_in ;
  librados::bufferlist id2_blist_out ;

  std::string str_reverse = "teststr" ;

  std::vector<std::pair<std::string, int>> data_reverse ;
  dataset_transform ds_reverse ;

  for( unsigned int i = 0; i < str_reverse.length(); i++ ) {
    std::string astr ;
    auto achar = str_reverse.c_str()[ i ] ;
    astr.push_back( achar ) ;
    ds_reverse.data.push_back( std::make_pair( astr, 1 ) ) ;
  } 

  std::vector< dataset_transform > table_reverse ;

  table_reverse.push_back( ds_reverse ) ;

  ::encode( table_reverse, id2_blist_in ) ;
  ceph_transform( &id2_blist_in, &id2_blist_out, 2 ) ;

  std::vector< dataset_transform > decoded_id2_blist_in ;
  librados::bufferlist::iterator decoded_id2_biter_in( &id2_blist_in ) ;
  ::decode( decoded_id2_blist_in, decoded_id2_biter_in ) ;
  std::vector< dataset_transform > decoded_id2_blist_out ;
  librados::bufferlist::iterator decoded_id2_biter_out( &id2_blist_out ) ;
  ::decode( decoded_id2_blist_out, decoded_id2_biter_out ) ;

  assert( decoded_id2_blist_in[0].toString()  == "dataset_transform :  t, 1  e, 1  s, 1  t, 1  s, 1  t, 1  r, 1" ) ;
  assert( decoded_id2_blist_out[0].toString() == "dataset_transform :  r, 1  t, 1  s, 1  t, 1  s, 1  e, 1  t, 1" ) ;

  // ----------------------------------------------------------------------------- //
  // ceph_transform_sort

  librados::bufferlist id3_blist_in ;
  librados::bufferlist id3_blist_out ;

  std::string str_sort = "teststr" ;

  std::vector<std::pair<std::string, int>> data_sort ;
  dataset_transform ds_sort ;

  for( unsigned int i = 0; i < str_sort.length(); i++ ) {
    std::string astr ;
    auto achar = str_sort.c_str()[ i ] ;
    astr.push_back( achar ) ;
    ds_sort.data.push_back( std::make_pair( astr, 1 ) ) ;
  } 

  std::vector< dataset_transform > table_sort ;

  table_sort.push_back( ds_sort ) ;

  ::encode( table_sort, id3_blist_in ) ;
  ceph_transform( &id3_blist_in, &id3_blist_out, 3 ) ;

  std::vector< dataset_transform > decoded_id3_blist_in ;
  librados::bufferlist::iterator decoded_id3_biter_in( &id3_blist_in ) ;
  ::decode( decoded_id3_blist_in, decoded_id3_biter_in ) ;
  std::vector< dataset_transform > decoded_id3_blist_out ;
  librados::bufferlist::iterator decoded_id3_biter_out( &id3_blist_out ) ;
  ::decode( decoded_id3_blist_out, decoded_id3_biter_out ) ;

  assert( decoded_id3_blist_in[0].toString()  == "dataset_transform :  t, 1  e, 1  s, 1  t, 1  s, 1  t, 1  r, 1" ) ;
  assert( decoded_id3_blist_out[0].toString() == "dataset_transform :  e, 1  r, 1  s, 1  s, 1  t, 1  t, 1  t, 1" ) ;

  // ----------------------------------------------------------------------------- //
  // ceph_transform_transpose

  librados::bufferlist id4_blist_in ;
  librados::bufferlist id4_blist_out ;

  std::string row1_data = "ABCD" ;
  std::string row2_data = "ABCD" ;
  std::string row3_data = "ABCD" ;

  std::vector<std::pair<std::string, int>> data1 ;
  std::vector<std::pair<std::string, int>> data2 ;
  std::vector<std::pair<std::string, int>> data3 ;
  std::vector<std::pair<std::string, int>> data4 ;
  dataset_transform ds1 ;
  dataset_transform ds2 ;
  dataset_transform ds3 ;

  for( unsigned int i = 0; i < row1_data.length(); i++ ) {
    std::string astr ;
    auto achar = row1_data.c_str()[ i ] ;
    astr.push_back( achar ) ;
    ds1.data.push_back( std::make_pair( astr, 1 ) ) ;
  } 
  for( unsigned int i = 0; i < row2_data.length(); i++ ) {
    std::string astr ;
    auto achar = row2_data.c_str()[ i ] ;
    astr.push_back( achar ) ;
    ds2.data.push_back( std::make_pair( astr, 2 ) ) ;
  } 
  for( unsigned int i = 0; i < row3_data.length(); i++ ) {
    std::string astr ;
    auto achar = row3_data.c_str()[ i ] ;
    astr.push_back( achar ) ;
    ds3.data.push_back( std::make_pair( astr, 3 ) ) ;
  } 

  //std::cout << "====================" << std::endl ;
  //std::cout << ds1.toString() << std::endl ;
  //std::cout << ds2.toString() << std::endl ;
  //std::cout << ds3.toString() << std::endl ;

  std::vector< dataset_transform > table ;

  table.push_back( ds1 ) ;
  table.push_back( ds2 ) ;
  table.push_back( ds3 ) ;

  ::encode( table, id4_blist_in ) ;
  ceph_transform( &id4_blist_in, &id4_blist_out, 4 ) ;

  std::vector< dataset_transform > decoded_id4_blist_in ;
  librados::bufferlist::iterator decoded_id4_biter_in( &id4_blist_in ) ;
  ::decode( decoded_id4_blist_in, decoded_id4_biter_in ) ;
  std::vector< dataset_transform > decoded_id4_blist_out ;
  librados::bufferlist::iterator decoded_id4_biter_out( &id4_blist_out ) ;
  ::decode( decoded_id4_blist_out, decoded_id4_biter_out ) ;

  //std::cout << "====================" << std::endl ;
  //std::cout << decoded_id4_blist_out[0].toString() << std::endl ;
  //std::cout << decoded_id4_blist_out[1].toString() << std::endl ;
  //std::cout << decoded_id4_blist_out[2].toString() << std::endl ;
  //std::cout << decoded_id4_blist_out[3].toString() << std::endl ;

  assert( decoded_id4_blist_in[0].toString()  == "dataset_transform :  A, 1  B, 1  C, 1  D, 1" ) ;
  assert( decoded_id4_blist_in[1].toString()  == "dataset_transform :  A, 2  B, 2  C, 2  D, 2" ) ;
  assert( decoded_id4_blist_in[2].toString()  == "dataset_transform :  A, 3  B, 3  C, 3  D, 3" ) ;
  assert( decoded_id4_blist_out[0].toString() == "dataset_transform :  A, 1  A, 2  A, 3" ) ;
  assert( decoded_id4_blist_out[1].toString() == "dataset_transform :  B, 1  B, 2  B, 3" ) ;
  assert( decoded_id4_blist_out[2].toString() == "dataset_transform :  C, 1  C, 2  C, 3" ) ;
  assert( decoded_id4_blist_out[3].toString() == "dataset_transform :  D, 1  D, 2  D, 3" ) ;

  // ----------------------------------------------------------------------------- //
  // ceph_transform_noop

//  librados::bufferlist id0_blist_in ;
//  librados::bufferlist id0_blist_out ;
//  id0_blist_in.append( "teststr", 7 ) ;
//
//  ceph_transform( &id0_blist_in, &id0_blist_out, 0 ) ;
//  assert( id3_blist_in.length() == 7 ) ;
//  assert( ((std::string)id3_blist_in.c_str()).compare("teststr" ) == 0 ) ;

  std::cout << "...ceph_transformss done. phew!" << std::endl ;
  return 0 ;
}
