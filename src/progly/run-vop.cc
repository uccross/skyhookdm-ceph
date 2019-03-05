/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/

#include <fstream>
#include <boost/program_options.hpp>
#include "vop_engine.h"

namespace po = boost::program_options;

int main(int argc, char **argv)
{
  // define relation
  dataset_transform ds0 ;
  dataset_transform ds1 ;
  dataset_transform ds2 ;

  // testrel |  name  |  num  |
  // --------------------------
  //            a        11
  //            b        12
  //            c        13
  ds0.data.push_back( std::make_pair( "a", 0 ) ) ;
  ds0.data.push_back( std::make_pair( "11", 0 ) ) ;
  ds0.schema.push_back( "name" ) ;
  ds0.schema.push_back( "num" ) ;

  ds1.data.push_back( std::make_pair( "b", 1 ) ) ;
  ds1.data.push_back( std::make_pair( "12", 1 ) ) ;
  ds1.schema.push_back( "name" ) ;
  ds1.schema.push_back( "num" ) ;

  ds2.data.push_back( std::make_pair( "c", 2 ) ) ;
  ds2.data.push_back( std::make_pair( "13", 2 ) ) ;
  ds2.schema.push_back( "name" ) ;
  ds2.schema.push_back( "num" ) ;

  std::vector< dataset_transform > testrel ;
  testrel.push_back( ds0 ) ;
  testrel.push_back( ds1 ) ;
  testrel.push_back( ds2 ) ;

  librados::bufferlist testrel_blist ;
  ::encode( testrel, testrel_blist ) ;
  for( unsigned int i = 0; i < testrel.size(); i++ ) {
    std::cout << testrel[i].toString() << " > schema " << testrel[i].schema 
                                       << " > arity " << testrel[i].schema.size() << std::endl ;
  }

  // define query : select name,num from testrel
  vop_query vq ;
  vq.relation_name = "testrel" ;
  vq.attribute_vect.push_back( "name" ) ;
  //vq.attribute_vect.push_back( "num" ) ;
  std::cout << vq.toString() << std::endl ;

  // execute query (working over bufferlists atm)
  execute_query( vq, testrel_blist ) ;

  return 0 ;
}
