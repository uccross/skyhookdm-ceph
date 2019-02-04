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

  // split_noop
  librados::bufferlist id0_blist_in ;
  librados::bufferlist id0_blist_out0 ;
  librados::bufferlist id0_blist_out1 ;
  id0_blist_in.append( "teststr", 7 ) ;

  Split::split( &id0_blist_in, &id0_blist_out0, &id0_blist_out1, 0 ) ;
  std::cout << "DRIVER check :"    << std::endl ;
  std::cout << id0_blist_in.c_str()    << std::endl ;
  std::cout << id0_blist_in.length()   << std::endl ;
  std::cout << id0_blist_out0.c_str()  << std::endl ;
  std::cout << id0_blist_out0.length() << std::endl ;
  std::cout << id0_blist_out1.c_str()  << std::endl ;
  std::cout << id0_blist_out1.length() << std::endl ;

  // split_12
  librados::bufferlist id1_blist_in ;
  librados::bufferlist id1_blist_out0 ;
  librados::bufferlist id1_blist_out1 ;
  id1_blist_in.append( "teststr", 7 ) ;

  Split::split( &id1_blist_in, &id1_blist_out0, &id1_blist_out1, 1 ) ;
  std::cout << "DRIVER check :"    << std::endl ;
  std::cout << id1_blist_in.c_str()    << std::endl ;
  std::cout << id1_blist_in.length()   << std::endl ;
  std::cout << id1_blist_out0.c_str()  << std::endl ;
  std::cout << id1_blist_out0.length() << std::endl ;
  std::cout << id1_blist_out1.c_str()  << std::endl ;
  std::cout << id1_blist_out1.length() << std::endl ;

  // split_pattern
  librados::bufferlist id2_blist_in ;
  librados::bufferlist id2_blist_out0 ;
  librados::bufferlist id2_blist_out1 ;
  id2_blist_in.append( "teststr", 7 ) ;

  //Split::split( &id1_blist_in, &id2_blist_out0, &id2_blist_out1, 2 ) ;
  std::cout << "DRIVER check :"    << std::endl ;
  std::cout << id2_blist_in.c_str()    << std::endl ;
  std::cout << id2_blist_in.length()   << std::endl ;
  std::cout << id2_blist_out0.c_str()  << std::endl ;
  std::cout << id2_blist_out0.length() << std::endl ;
  std::cout << id2_blist_out1.c_str()  << std::endl ;
  std::cout << id2_blist_out1.length() << std::endl ;

  std::cout << "...splits done. phew!" << std::endl ;
  return 0 ;
}
