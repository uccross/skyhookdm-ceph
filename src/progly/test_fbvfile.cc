/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#include "cls/tabular/cls_test_utils.h"
#include <boost/lexical_cast.hpp>

int main( int argc, char **argv ) {
  std::cout << "in test_fbvfile.cc..." << std::endl ;

  std::vector< uint64_t > int_vect ;
  std::ifstream ifs( "/home/minion/projects/skyhookdb-ceph/src/progly/test_fbvfile_10.csv" ) ;
  std::string line ;
  // while loop based on https://stackoverflow.com/a/1474819:
  while( std::getline( ifs, line ) ) {
    std::stringstream linestream( line ) ;
    std::string       value ;
    while( std::getline( linestream, value, '\n' ) ) {
      std::cout << "Value(" << value << ")\n" ;
      int_vect.push_back( boost::lexical_cast<uint64_t>( value.c_str() ) ) ;
    }
    //std::cout << "Line Finished" << std::endl ;
  }

  std::cout << "int_vect:" << std::endl ;
  std::cout << int_vect << std::endl ;

  // save to Basic flatbuffer
  save_to_basic_fb( int_vect ) ;

  // read from Basic flatbuffer as raw vector
  read_basic_fb( "test_fbvfile_10" ) ;

  // save to file
  //

  // read from file as raw vector
  //

  return 0 ;
}
