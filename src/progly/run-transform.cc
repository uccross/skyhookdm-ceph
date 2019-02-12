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
  id1_blist_in.append( "teststr", 7 ) ;

  //Transform::ceph_transform( &id1_blist_in, &id1_blist_out, 1 ) ;
  ceph_transform( &id1_blist_in, &id1_blist_out, 1 ) ;
  assert( id1_blist_in.length()  == 7 ) ;
  assert( id1_blist_out.length() == 7 ) ;
  assert( ((std::string)id1_blist_in.c_str()).compare( "teststr" )  == 0 ) ;
  assert( ((std::string)id1_blist_out.c_str()).compare( "teststr" ) == 0 ) ;

  // ----------------------------------------------------------------------------- //
  // ceph_transform_reverse

  librados::bufferlist id2_blist_in ;
  librados::bufferlist id2_blist_out ;
  id2_blist_in.append( "teststr", 7 ) ;

  //Transform::ceph_transform( &id2_blist_in, &id2_blist_out, 2 ) ;
  ceph_transform( &id2_blist_in, &id2_blist_out, 2 ) ;
  assert( id2_blist_in.length()  == 7 ) ;
  assert( id2_blist_out.length() == 7 ) ;
  assert( ((std::string)id2_blist_in.c_str()).compare(  "teststr" ) == 0 ) ;
  assert( ((std::string)id2_blist_out.c_str()).compare( "rtstset" ) == 0 ) ;

  // ----------------------------------------------------------------------------- //
  // ceph_transform_sort

  librados::bufferlist id3_blist_in ;
  librados::bufferlist id3_blist_out ;
  id3_blist_in.append( "teststr", 7 ) ;

  //Transform::ceph_transform( &id3_blist_in, &id3_blist_out, 3 ) ;
  ceph_transform( &id3_blist_in, &id3_blist_out, 3 ) ;
  assert( id3_blist_in.length()  == 7 ) ;
  assert( id3_blist_out.length() == 7 ) ;
  assert( ((std::string)id3_blist_in.c_str()).compare(  "teststr" ) == 0 ) ;
  assert( ((std::string)id3_blist_out.c_str()).compare( "erssttt" ) == 0 ) ;

  // ----------------------------------------------------------------------------- //
  // ceph_transform_transpose

  librados::bufferlist id4_blist_in ;
  librados::bufferlist id4_blist_out ;

  std::string row1_data = "ABC" ;
  std::string row2_data = "ABC" ;
  std::string row3_data = "ABC" ;

  dataset ds1 ;
  dataset ds2 ;
  dataset ds3 ;

  ds1.id = 1 ;
  ds2.id = 2 ;
  ds3.id = 3 ;

  ds1.data = row1_data ;
  ds2.data = row2_data ;
  ds3.data = row3_data ;

  std::vector< dataset > table ;

  table.push_back( ds1 ) ;
  table.push_back( ds2 ) ;
  table.push_back( ds3 ) ;

  ::encode( table, id4_blist_in ) ;
  ceph_transform( &id4_blist_in, &id4_blist_out, 4 ) ;

  std::vector< dataset > decoded_id4_blist_in ;
  librados::bufferlist::iterator decoded_id4_biter_in( &id4_blist_in ) ;
  ::decode( decoded_id4_blist_in, decoded_id4_biter_in ) ;
  std::vector< dataset > decoded_id4_blist_out ;
  librados::bufferlist::iterator decoded_id4_biter_out( &id4_blist_out ) ;
  ::decode( decoded_id4_blist_out, decoded_id4_biter_out ) ;

  assert( decoded_id4_blist_in[0].toString()  == "dataset : .id   = 1 .data = ABC"  ) ;
  assert( decoded_id4_blist_in[1].toString()  == "dataset : .id   = 2 .data = ABC"  ) ;
  assert( decoded_id4_blist_in[2].toString()  == "dataset : .id   = 3 .data = ABC"  ) ;
  assert( decoded_id4_blist_out[0].toString() == "dataset : .id   = 1 .data = AAA"  ) ;
  assert( decoded_id4_blist_out[1].toString() == "dataset : .id   = 2 .data = BBB"  ) ;
  assert( decoded_id4_blist_out[2].toString() == "dataset : .id   = 3 .data = CCC"  ) ;

  // ----------------------------------------------------------------------------- //
  // ceph_transform_noop

  librados::bufferlist id0_blist_in ;
  librados::bufferlist id0_blist_out ;
  id0_blist_in.append( "teststr", 7 ) ;

  ceph_transform( &id0_blist_in, &id0_blist_out, 0 ) ;
  assert( id3_blist_in.length() == 7 ) ;
  assert( ((std::string)id3_blist_in.c_str()).compare("teststr" ) == 0 ) ;

  std::cout << "...ceph_transformss done. phew!" << std::endl ;
  return 0 ;
}
