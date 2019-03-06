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
#include "cls/dm/cls_thing.h"
#include "cls/dm/cls_split.h"

namespace po = boost::program_options;

int main(int argc, char **argv)
{
  std::cout << "starting run-dm.cc ..." << std::endl ;

// =================================================================== //
  afunc() ;
  thing2() ;
  // str
  Dataset ds( 0 ) ;
  std::cout << "type_code = " << ds.get_type_code() << std::endl ;
  ds.set_dataset( "asdf" ) ;
  std::cout << "printed dataset:" << std::endl ;
  ds.print_dataset() ;
// =================================================================== //

  // vector of vector of pairs
  std::string row1_data = "ABCD" ;
  std::string row2_data = "ABCD" ;
  std::string row3_data = "ABCD" ;

  std::vector<std::pair<std::string, int>> data1 ;
  std::vector<std::pair<std::string, int>> data2 ;
  std::vector<std::pair<std::string, int>> data3 ;
  std::vector<std::pair<std::string, int>> data4 ;
  dataset_struct ds1 ;
  dataset_struct ds2 ;
  dataset_struct ds3 ;

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

  std::vector< dataset_struct > table ;

  table.push_back( ds1 ) ;
  table.push_back( ds2 ) ;
  table.push_back( ds3 ) ;

  Dataset dset3( 3 ) ;
  dset3.set_dataset( table ) ;
  dset3.print_dataset() ;

  std::cout << "... run-dm.cc done. phew!" << std::endl ;
  return 0 ;
}
