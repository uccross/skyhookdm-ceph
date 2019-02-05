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

// #define NDEBUG // uncomment to disable assert()
#include <cassert>

namespace po = boost::program_options;

int main(int argc, char **argv)
{
  std::cout << "starting transforms..." << std::endl ;

  // transform_all
  librados::bufferlist id1_blist_in ;
  librados::bufferlist id1_blist_out ;
  id1_blist_in.append( "teststr", 7 ) ;

  Transform::transform( &id1_blist_in, &id1_blist_out, 1 ) ;
  assert( id1_blist_in.length()  == 7 ) ;
  assert( id1_blist_out.length() == 7 ) ;
  assert( ((std::string)id1_blist_in.c_str()).compare("teststr" ) == 0  ) ;
  assert( ((std::string)id1_blist_out.c_str()).compare("teststr" ) == 0  ) ;

  // transform_reverse
  librados::bufferlist id2_blist_in ;
  librados::bufferlist id2_blist_out ;
  id2_blist_in.append( "teststr", 7 ) ;

  Transform::transform( &id2_blist_in, &id2_blist_out, 2 ) ;
  assert( id2_blist_in.length()  == 7 ) ;
  assert( id2_blist_out.length() == 7 ) ;
  assert( ((std::string)id2_blist_in.c_str()).compare("teststr" ) == 0  ) ;
  assert( ((std::string)id2_blist_out.c_str()).compare("rtstset" ) == 0  ) ;

  // transform_sort
  librados::bufferlist id3_blist_in ;
  librados::bufferlist id3_blist_out ;
  id3_blist_in.append( "teststr", 7 ) ;

  Transform::transform( &id3_blist_in, &id3_blist_out, 3 ) ;
  assert( id3_blist_in.length()  == 7 ) ;
  assert( id3_blist_out.length() == 7 ) ;
  assert( ((std::string)id3_blist_in.c_str()).compare("teststr" ) == 0  ) ;
  assert( ((std::string)id3_blist_out.c_str()).compare("erssttt" ) == 0  ) ;

  // transform_noop
  librados::bufferlist id0_blist_in ;
  librados::bufferlist id0_blist_out ;
  id0_blist_in.append( "teststr", 7 ) ;

  Transform::transform( &id0_blist_in, &id0_blist_out, 0 ) ;
  assert( id3_blist_in.length() == 7 ) ;
  assert( ((std::string)id3_blist_in.c_str()).compare("teststr" ) == 0  ) ;

  std::cout << "...transformss done. phew!" << std::endl ;
  return 0 ;
}
