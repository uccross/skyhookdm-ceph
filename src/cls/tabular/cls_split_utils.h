/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#ifndef CLS_SPLIT_UTILS_H
#define CLS_SPLIT_UTILS_H

#include <string>

#include "include/rados/librados.hpp"
#include "flatbuffers/flatbuffers.h"

// https://stackoverflow.com/questions/3247861/example-of-uuid-generation-using-boost-in-c
#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.
#include <boost/lexical_cast.hpp>

namespace SplitUtils {

  // =========================================================== //
  // DO WRITE

  /* Write an object synchronously. */
  void do_write( librados::IoCtx io_ctx, std::string obj_name, std::string obj_data ) 
  {
    {
      librados::bufferlist bl;
      bl.append( obj_data );
      int ret = io_ctx.write_full( obj_name, bl ) ;
      if (ret < 0) {
        std::cerr << "Couldn't write object! error " << ret << std::endl;
        exit(EXIT_FAILURE);
      } else {
        std::cout << "Wrote new object '"  << obj_name << "' " << std::endl;
      }
    }
  }

  // =========================================================== //
  // RM ORIG

  /* Remove the object. */
  void rm_orig( librados::IoCtx io_ctx, const char *obj_name )
  {
    {
      int ret = io_ctx.remove( obj_name ) ;
      if (ret < 0) {
        std::cerr << "Couldn't remove object! error " << ret << std::endl;
        exit(EXIT_FAILURE);
      } else {
        std::cout << "Removed object '" << obj_name << "'." << std::endl;
      }
    }
  }

  // =========================================================== //
  // DO SPLIT

  void do_split( librados::IoCtx io_ctx, const char *obj_name, librados::bufferlist read_buf )
  {
    std::cout << "SplitUtils do_split()" << std::endl ;
    std::cout << "obj_name = " << obj_name << std::endl ;
    std::cout << "read_buf = " << read_buf.c_str() << std::endl ;

    // split data
    std::string obj_data = read_buf.c_str() ;
    int split_index = obj_data.length() / 2 ;

    std::string left_data  = obj_data.substr( 0, split_index ) ;
    std::string right_data = obj_data.substr( split_index ) ;

    std::cout << left_data << std::endl ;
    std::cout << right_data << std::endl ;

    // new split names
    boost::uuids::random_generator generator;
    boost::uuids::uuid uuid0 = generator();
    std::cout << uuid0 << std::endl;
    boost::uuids::uuid uuid1 = generator();
    std::cout << uuid1 << std::endl;

    std::string tmp0 = boost::lexical_cast<std::string>( uuid0 ) ;
    std::string split_obj_name0 = obj_name + tmp0 ;
    std::cout << split_obj_name0 << std::endl ;

    std::string tmp1 = boost::lexical_cast<std::string>( uuid1 ) ;
    std::string split_obj_name1 = obj_name + tmp1 ;
    std::cout << split_obj_name1 << std::endl ;

    do_write( io_ctx, split_obj_name0, left_data ) ;
    do_write( io_ctx, split_obj_name1, right_data ) ;

    rm_orig( io_ctx, obj_name ) ;

  } // do_split

}

#endif
