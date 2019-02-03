/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#ifndef CLS_SPLITS_H
#define CLS_SPLITS_H

#include <string>

#include "include/rados/librados.hpp"
#include "cls/copy_transform/cls_copy_transform.h"

// https://stackoverflow.com/questions/3247861/example-of-uuid-generation-using-boost-in-c
#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.
#include <boost/lexical_cast.hpp>
#include <tuple>

namespace Splits {

  void test_func()
  {
    std::cout << "executed Splits func" << std::endl ;
  }

  // =========================================================== //
  // PROTOTYPES


  // =========================================================== //
  // SPLIT

  // src_bptr
  // split_number => divide the given data into "split_number" 
  // parts of approximately equal sizes
  int split( librados::bufferlist::iterator src_biter, librados::bufferlist dest_blist, int split_number = 0 )
  {
    // split into equal parts
    if( split_number > 0 )
    { 
      std::cout << "split_number > 0" << std::endl ;
    }
    else //copy entire bufferlist
    {
      const buffer::ptr src_bptr = src_biter.get_current_ptr();
      std::cout << src_bptr[0] << std::endl ;
      std::cout << src_bptr[1] << std::endl ;
      std::cout << src_bptr[2] << std::endl ;
      std::cout << src_bptr[3] << std::endl ;
      //std::cout << src_bptr[4] << std::endl ; //core dump
  
      std::cout << "src_bptr.offset()         = " << src_bptr.offset()         << std::endl ;
      std::cout << "src_bptr.length()         = " << src_bptr.length()         << std::endl ;
      std::cout << "src_biter.get_remaining() = " << src_biter.get_remaining() << std::endl ; 
      std::cout << "src_biter.get_off()       = " << src_biter.get_off()       << std::endl ; 
  
      std::string dest_data = "" ;
      for ( unsigned int i = 0; i < src_bptr.length(); i++ )
      {
        dest_data += src_bptr[i] ;
      }
      std::cout << "dest_data = " << dest_data << std::endl ;
  
      bufferptr b( dest_data.c_str(), (signed int)src_bptr.length() ) ;
      dest_blist.append( b ) ;
      //std::cout << "dest_blist.c_str() = " << dest_blist.c_str() << std::endl ;
      //bufferlist::iterator dest_biter = dest_blist.begin() ;
      //std::cout << "dest_biter.get_remaining() = " << dest_biter.get_remaining() << std::endl ;
    }

    return 0 ;
  }

}

#endif
