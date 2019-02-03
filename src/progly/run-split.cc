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
#include "re2/re2.h"

// ceph includes
#include "include/rados/librados.hpp"

// skyhook includes
#include "query.h"
#include "cls/copy_transform/cls_splits.h"

#include <chrono>

namespace po = boost::program_options;

int main(int argc, char **argv)
{

  std::cout << "starting splits..." << std::endl ;

  CopyTransform::test_func() ;
  Splits::test_func() ;

  // Copy from iter into target bl
  {
    std::cout << "run-split.cc" << std::endl ;

    bufferlist bl;
    bl.append("ABCD",4);
    bufferlist copy;
    bufferlist::iterator i(&bl);  // you can also declare iterator at a pos:  i(&bl, 2);
    i.copy(1, copy);  // copy="A"
    i.seek(0);
    i.copy(4, copy);  // copy=AABCD
    i.seek(2);
    i.copy(1, copy) ;  // copy=AABCDC
    i.seek(0);
    i.copy(4, copy);  // copy=AABCDCABCD

    std::cout << bl.c_str() << std::endl ;
    std::cout << copy.c_str() << std::endl ;
  }

  std::cout << "==========================================" << std::endl ;
  std::cout << ">>>   NO SPLIT   <<<" << std::endl ;

  {
//    bufferlist src_blist ;
//    bufferptr a("asdf", 4);
//    src_blist.append( a ) ;
//    bufferlist dest_blist ;
//    bufferlist::iterator src_biter = src_blist.begin() ;
//
//    Splits::split( src_biter, dest_blist, 0 ) ;
//
//    std::cout << "check these:" << std::endl ;
//    std::cout << "dest_blist.c_str() = " << dest_blist.c_str() << std::endl ;
//    bufferlist::iterator dest_biter = dest_blist.begin() ;
//    std::cout << "dest_biter.get_remaining() = " << dest_biter.get_remaining() << std::endl ;

    //bufferlist src_blist ;
    //bufferptr a("asdf", 4);
    //src_blist.append( a ) ;
    //bufferlist dest_blist ;
    //bufferlist::iterator src_biter = src_blist.begin() ;
    //const buffer::ptr src_bptr = src_biter.get_current_ptr();
    //std::cout << src_bptr[0] << std::endl ;
    //std::cout << src_bptr[1] << std::endl ;
    //std::cout << src_bptr[2] << std::endl ;
    //std::cout << src_bptr[3] << std::endl ;
    ////std::cout << src_bptr[4] << std::endl ; //core dump

    //std::cout << "src_bptr.offset()         = " << src_bptr.offset()         << std::endl ;
    //std::cout << "src_bptr.length()         = " << src_bptr.length()         << std::endl ;
    //std::cout << "src_biter.get_remaining() = " << src_biter.get_remaining() << std::endl ; 
    //std::cout << "src_biter.get_off()       = " << src_biter.get_off()       << std::endl ; 

    //std::string dest_data = "" ;
    //for ( unsigned int i = 0; i < src_bptr.length(); i++ )
    //{
    //  dest_data += src_bptr[i] ;
    //}
    //std::cout << "dest_data = " << dest_data << std::endl ;

    //bufferptr b( dest_data.c_str(), (signed int)src_bptr.length() ) ;
    //dest_blist.append( b ) ;
    //std::cout << "dest_blist.c_str() = " << dest_blist.c_str() << std::endl ;
    //bufferlist::iterator dest_biter = dest_blist.begin() ;
    //std::cout << "dest_biter.get_remaining() = " << dest_biter.get_remaining() << std::endl ;

    ////src_biter.copy( 1, dest_blist ) ;
    ////std::cout << " src_blist.c_str()  = " << src_blist.c_str() << std::endl ;
    ////std::cout << " dest_blist.c_str() = " << dest_blist.c_str() << std::endl ;
  }

  std::cout << "==========================================" << std::endl ;
  std::cout << ">>>   HALF SPLIT   <<<" << std::endl ;

  std::cout << "==========================================" << std::endl ;
  std::cout << ">>>   THIRD SPLIT   <<<" << std::endl ;

  std::cout << "==========================================" << std::endl ;
  std::cout << ">>>   QUARTER SPLIT   <<<" << std::endl ;

  std::cout << "...splits done. phew!" << std::endl ;
  return 0 ;
}
