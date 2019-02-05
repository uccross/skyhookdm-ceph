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
  id1_blist_in.append( "teststr", 7 ) ;

  Split::split( &id1_blist_in, &id1_blist_out0, &id1_blist_out1, 1 ) ;
  assert( id1_blist_in.length()   == 7 ) ;
  assert( id1_blist_out0.length() == 3 ) ;
  assert( id1_blist_out1.length() == 4 ) ;
  assert( ((std::string)id1_blist_in.c_str()).compare(   "teststr" ) == 0 ) ;
  assert( ((std::string)id1_blist_out0.c_str()).compare( "tes"     ) == 0 ) ;
  assert( ((std::string)id1_blist_out1.c_str()).compare( "tstr"    ) == 0 ) ;

  // split_pattern
  librados::bufferlist id2_blist_in ;
  librados::bufferlist id2_blist_out0 ;
  librados::bufferlist id2_blist_out1 ;
  id2_blist_in.append( "teststr", 7 ) ;

  Split::split( &id1_blist_in, &id2_blist_out0, &id2_blist_out1, 2 ) ;
  assert( id2_blist_in.length()   == 7 ) ;
  assert( id2_blist_out0.length() == 2 ) ;
  assert( id2_blist_out1.length() == 5 ) ;
  assert( ((std::string)id2_blist_in.c_str()).compare(   "teststr" ) == 0 ) ;
  assert( ((std::string)id2_blist_out0.c_str()).compare( "te"      ) == 0 ) ;
  assert( ((std::string)id2_blist_out1.c_str()).compare( "ststr"   ) == 0 ) ;

  // split_noop
  librados::bufferlist id0_blist_in ;
  librados::bufferlist id0_blist_out0 ;
  librados::bufferlist id0_blist_out1 ;
  id0_blist_in.append( "teststr", 7 ) ;

  Split::split( &id0_blist_in, &id0_blist_out0, &id0_blist_out1, 0 ) ;
  assert( id0_blist_in.length()   == 7 ) ;
  assert( id0_blist_out0.length() == 7 ) ;
  assert( id0_blist_out1.length() == 7 ) ;
  assert( ((std::string)id0_blist_in.c_str()).compare(   "teststr" ) == 0 ) ;
  assert( ((std::string)id0_blist_out0.c_str()).compare( "teststr" ) == 0 ) ;
  assert( ((std::string)id0_blist_out1.c_str()).compare( "teststr" ) == 0 ) ;

  std::cout << "...splits done. phew!" << std::endl ;
  return 0 ;
}
