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
  Dataset avop( thing2, thing2schema ) ;
  std::cout << avop.toString() << std::endl ;

  // split
  int astr_mid   = get_midpoint( astr ) ;
  int thing0_mid = get_midpoint( thing0 ) ;
  int thing1_mid = get_midpoint( thing1 ) ;
  int avop_mid   = get_midpoint( avop ) ;

  std::cout << "astr_mid   : " << astr_mid   << std::endl ;
  std::cout << "thing0_mid : " << thing0_mid << std::endl ;
  std::cout << "thing1_mid : " << thing1_mid << std::endl ;
  std::cout << "avop_mid   : " << avop_mid   << std::endl ;

  std::cout << "... run-dm.cc done. phew!" << std::endl ;

  return 0 ;
}
