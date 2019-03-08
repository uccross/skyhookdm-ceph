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
#include "cls/dm/cls_dm.h"
#include "cls/dm/cls_split.h"

namespace po = boost::program_options;

int main(int argc, char **argv)
{
  std::cout << "starting run-dm.cc ..." << std::endl ;

  // string
  Dataset astr( "testing" ) ;
  std::cout << astr.toString() << std::endl ;

  // vector< string >
  std::vector< std::string > thing0 ;
  thing0.push_back( "aaa" ) ;
  thing0.push_back( "bbb" ) ;
  thing0.push_back( "ccc" ) ;
  Dataset avectstr( thing0 ) ;
  std::cout << avectstr.toString() << std::endl ;

  // vector< uint64 >
  std::vector< uint64_t > thing1 ;
  thing1.push_back( 0 ) ;
  thing1.push_back( 1 ) ;
  thing1.push_back( 2 ) ;
  thing1.push_back( 3 ) ;
  Dataset avectint( thing1 ) ;
  std::cout << avectint.toString() << std::endl ;

  // vector< pair< string, int > >
  // vector< string >
  std::vector< std::vector< std::pair< std::string, int > > > thing2 ;
  std::vector< std::pair< std::string, int > > row0 ;
  std::vector< std::pair< std::string, int > > row1 ;
  std::vector< std::pair< std::string, int > > row2 ;
  std::vector< std::pair< std::string, int > > row3 ;
  std::vector< std::pair< std::string, int > > row4 ;
  row0.push_back( std::make_pair( "aaa", 0 ) ) ;
  row0.push_back( std::make_pair( "aaa", 1 ) ) ;
  row0.push_back( std::make_pair( "aaa", 2 ) ) ;
  row1.push_back( std::make_pair( "bbb", 0 ) ) ;
  row1.push_back( std::make_pair( "bbb", 1 ) ) ;
  row1.push_back( std::make_pair( "bbb", 2 ) ) ;
  row2.push_back( std::make_pair( "ccc", 0 ) ) ;
  row2.push_back( std::make_pair( "ccc", 1 ) ) ;
  row2.push_back( std::make_pair( "ccc", 2 ) ) ;
  row3.push_back( std::make_pair( "ddd", 0 ) ) ;
  row3.push_back( std::make_pair( "ddd", 1 ) ) ;
  row3.push_back( std::make_pair( "ddd", 2 ) ) ;
  row4.push_back( std::make_pair( "eee", 0 ) ) ;
  row4.push_back( std::make_pair( "eee", 1 ) ) ;
  row4.push_back( std::make_pair( "eee", 2 ) ) ;
  thing2.push_back( row0 ) ;
  thing2.push_back( row1 ) ;
  thing2.push_back( row2 ) ;
  thing2.push_back( row3 ) ;
  thing2.push_back( row4 ) ;
  std::vector< std::string > thing2schema ;
  thing2schema.push_back( "col0" ) ;
  thing2schema.push_back( "col1" ) ;
  thing2schema.push_back( "col2" ) ;
  Dataset avvop( thing2, thing2schema ) ;
  std::cout << avvop.toString() << std::endl ;

  // split
  int astr_mid   = get_midpoint( astr ) ;
  int thing0_mid = get_midpoint( thing0 ) ;
  int thing1_mid = get_midpoint( thing1 ) ;
  int avvop_mid  = get_midpoint( avvop ) ;

  std::cout << "astr_mid   : " << astr_mid   << std::endl ;
  std::cout << "thing0_mid : " << thing0_mid << std::endl ;
  std::cout << "thing1_mid : " << thing1_mid << std::endl ;
  std::cout << "avvop_mid  : " << avvop_mid   << std::endl ;

  Dataset astr_range   = get_range( astr,   0, astr_mid   ) ;
  Dataset thing0_range = get_range( thing0, 0, thing0_mid ) ;
  Dataset thing1_range = get_range( thing1, 0, thing1_mid ) ;
  Dataset avvop_range  = get_range( avvop,  0, avvop_mid  ) ;

  std::cout << "astr_range   : \n" << astr_range.toString()   << std::endl ;
  std::cout << "thing0_range : \n" << thing0_range.toString() << std::endl ;
  std::cout << "thing1_range : \n" << thing1_range.toString() << std::endl ;
  std::cout << "avvop_range  : \n" << avvop_range.toString()   << std::endl ;

  // ------------------------------------------------------ //

  librados::bufferlist astr_split_blist_in ;
  librados::bufferlist astr_split_blist_out0 ;
  librados::bufferlist astr_split_blist_out1 ;

  // str split_noop
  ::encode( astr, astr_split_blist_in ) ;
  ceph_split( &astr_split_blist_in, 
              &astr_split_blist_out0, 
              &astr_split_blist_out1, 
              astr.type_code, -1 ) ;

  // str split_id
  ::encode( astr, astr_split_blist_in ) ;
  ceph_split( &astr_split_blist_in, 
              &astr_split_blist_out0, 
              &astr_split_blist_out1, 
              astr.type_code, 0 ) ;

  // str split_12
  ::encode( astr, astr_split_blist_in ) ;
  ceph_split( &astr_split_blist_in, 
              &astr_split_blist_out0, 
              &astr_split_blist_out1, 
              astr.type_code, 1 ) ;

  // str split_pattern
  ::encode( astr, astr_split_blist_in ) ;
  ceph_split( &astr_split_blist_in, 
              &astr_split_blist_out0, 
              &astr_split_blist_out1, 
              astr.type_code, 2, "i" ) ;

  // str split_hash2
  ::encode( astr, astr_split_blist_in ) ;
  ceph_split( &astr_split_blist_in, 
              &astr_split_blist_out0, 
              &astr_split_blist_out1, 
              astr.type_code, 3 ) ;

  // ------------------------------------------------------ //

  librados::bufferlist avectstr_split_blist_in ;
  librados::bufferlist avectstr_split_blist_out0 ;
  librados::bufferlist avectstr_split_blist_out1 ;

  // vect< str > split_noop
  ::encode( avectstr, avectstr_split_blist_in ) ;
  ceph_split( &avectstr_split_blist_in, 
              &avectstr_split_blist_out0, 
              &avectstr_split_blist_out1, 
              avectstr.type_code, -1 ) ;

  // vect< str > split_id
  ::encode( avectstr, avectstr_split_blist_in ) ;
  ceph_split( &avectstr_split_blist_in, 
              &avectstr_split_blist_out0, 
              &avectstr_split_blist_out1, 
              avectstr.type_code, 0 ) ;

  // vect< str > split_12
  ::encode( avectstr, avectstr_split_blist_in ) ;
  ceph_split( &avectstr_split_blist_in, 
              &avectstr_split_blist_out0, 
              &avectstr_split_blist_out1, 
              avectstr.type_code, 1 ) ;

  // vect< str > split_pattern
  ::encode( avectstr, avectstr_split_blist_in ) ;
  ceph_split( &avectstr_split_blist_in, 
              &avectstr_split_blist_out0, 
              &avectstr_split_blist_out1, 
              avectstr.type_code, 2, "bbb" ) ;

  // vect< str > split_hash2
  ::encode( avectstr, avectstr_split_blist_in ) ;
  ceph_split( &avectstr_split_blist_in, 
              &avectstr_split_blist_out0, 
              &avectstr_split_blist_out1, 
              avectstr.type_code, 3 ) ;

  // ------------------------------------------------------ //

  librados::bufferlist avectint_split_blist_in ;
  librados::bufferlist avectint_split_blist_out0 ;
  librados::bufferlist avectint_split_blist_out1 ;

  // vect< unint64 > split_noop
  ::encode( avectint, avectint_split_blist_in ) ;
  ceph_split( &avectint_split_blist_in, 
              &avectint_split_blist_out0, 
              &avectint_split_blist_out1, 
              avectint.type_code, -1 ) ;

  // vect< uint64 > split_id
  ::encode( avectint, avectint_split_blist_in ) ;
  ceph_split( &avectint_split_blist_in, 
              &avectint_split_blist_out0, 
              &avectint_split_blist_out1, 
              avectint.type_code, 0 ) ;

  // vect< uint64 > split_12
  ::encode( avectint, avectint_split_blist_in ) ;
  ceph_split( &avectint_split_blist_in, 
              &avectint_split_blist_out0, 
              &avectint_split_blist_out1, 
              avectint.type_code, 1 ) ;

  // vect< uint64 > split_pattern
  ::encode( avectint, avectint_split_blist_in ) ;
  ceph_split( &avectint_split_blist_in, 
              &avectint_split_blist_out0, 
              &avectint_split_blist_out1, 
              avectint.type_code, 2, 2 ) ;

  // vect< uint64 > split_hash2
  ::encode( avectint, avectint_split_blist_in ) ;
  ceph_split( &avectint_split_blist_in, 
              &avectint_split_blist_out0, 
              &avectint_split_blist_out1, 
              avectint.type_code, 3 ) ;
  // ------------------------------------------------------ //

  librados::bufferlist avvop_split_blist_in ;
  librados::bufferlist avvop_split_blist_out0 ;
  librados::bufferlist avvop_split_blist_out1 ;

  // vvop split_noop
  ::encode( avvop, avvop_split_blist_in ) ;
  ceph_split( &avvop_split_blist_in, 
              &avvop_split_blist_out0, 
              &avvop_split_blist_out1, 
              avvop.type_code, -1 ) ;

  // vvop split_id
  std::cout << avvop.toString() << std::endl ;
  ::encode( avvop, avvop_split_blist_in ) ;
  ceph_split( &avvop_split_blist_in, 
              &avvop_split_blist_out0, 
              &avvop_split_blist_out1, 
              avvop.type_code, 0 ) ;

  // vvop split_12
  ::encode( avvop, avvop_split_blist_in ) ;
  ceph_split( &avvop_split_blist_in, 
              &avvop_split_blist_out0, 
              &avvop_split_blist_out1, 
              avvop.type_code, 1 ) ;

  // vvop split_pattern (wait on this one. this is a query.)
  //::encode( avvop, avvop_split_blist_in ) ;
  //ceph_split( &avvop_split_blist_in, 
  //            &avvop_split_blist_out0, 
  //            &avvop_split_blist_out1, 
  //            avvop.type_code, 2, std::make_pair( "ccc" ) ) ;

  // vvop split_hash2
  ::encode( avvop, avvop_split_blist_in ) ;
  ceph_split( &avvop_split_blist_in, 
              &avvop_split_blist_out0, 
              &avvop_split_blist_out1, 
              avvop.type_code, 3 ) ;

  std::cout << "... run-dm.cc done. phew!" << std::endl ;

  return 0 ;
}
