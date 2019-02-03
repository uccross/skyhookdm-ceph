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
#include <tuple>

namespace SplitUtils {

  void do_write( librados::IoCtx io_ctx, 
                 std::string obj_name, 
                 librados::bufferlist obj_bl ) ; 

  librados::bufferlist do_read( librados::IoCtx io_ctx, 
                                std::string obj_name );

  librados::bufferlist copy_range2( librados::IoCtx io_ctx, 
                                    std::string src_oid,
                                    std::string dst_oid,
                                    int start_off,
                                    int end_off ) ;

  librados::bufferlist copy_range1( librados::IoCtx io_ctx, 
                                    std::string src_oid,
                                    std::string dst_oid,
                                    int start_off,
                                    int end_off ) ;

  void do_remove( librados::IoCtx io_ctx, std::string oid ) ;

  // =========================================================== //
  // TRANSFORM

  int transform( librados::IoCtx io_ctx, 
                 std::string src_oid, 
                 int offset, 
                 int len, 
                 int (*transform_func)( librados::IoCtx io_ctx, librados::bufferlist, int, int ) )
  {
    std::cout << "\n============\nrunning transform\n\n" << std::endl ;
    std::cout << "    src_oid      = " << src_oid << std::endl ;
    std::cout << "    offset       = " << offset  << std::endl ;
    std::cout << "    len          = " << len     << std::endl ;
    std::cout << "\n" << std::endl ;

    librados::bufferlist src_bl  = do_read( io_ctx, src_oid ) ;

    std::cout << "------------- start -------------" << std::endl ;

    std::string data = (std::string)src_bl.c_str() ;

    std::cout << data   << std::endl ;
    std::cout << len    << std::endl ;
    std::cout << offset << std::endl ;

    transform_func( io_ctx, src_bl, offset, len ) ;

    std::cout << "------------- end -------------" << std::endl ;

    librados::bufferlist _src_bl  = do_read( io_ctx, src_oid ) ;

    //do_remove( io_ctx, src_oid ) ;

    return 0 ;
  }


  // =========================================================== //
  // SPLIT 2

  int split2( librados::IoCtx io_ctx, std::string src_oid )
  {
    std::cout << "\n============\nrunning split2\n\n" << std::endl ;
    std::cout << "    src_oid      = " << src_oid << std::endl ;
    std::cout << "\n" << std::endl ;

    // write new empty objects
    std::string src_oid0 = src_oid + "_split20" ;
    std::string src_oid1 = src_oid + "_split21" ;
    const char* dst0_oid = src_oid0.c_str() ;
    const char* dst1_oid = src_oid1.c_str() ;

    std::cout << "------------- start -------------" << std::endl ;

    librados::bufferlist src_bl  = do_read( io_ctx, src_oid ) ;

    std::string data = (std::string)src_bl.c_str() ;
    int len          = data.size() ;
    int offset       = len / 2 ;

    std::cout << data << std::endl ;
    std::cout << len << std::endl ;
    std::cout << offset << std::endl ;

    librados::bufferlist dst0_data ;
    src_bl.copy( 0, offset, dst0_data ) ;
    do_write( io_ctx, dst0_oid, dst0_data ) ;

    librados::bufferlist dst1_data ;
    src_bl.copy( offset, len - offset, dst1_data ) ;
    do_write( io_ctx, dst1_oid, dst1_data ) ;

    std::cout << "------------- end -------------" << std::endl ;

    librados::bufferlist _src_bl  = do_read( io_ctx, src_oid ) ;
    librados::bufferlist _dst0_bl = do_read( io_ctx, dst0_oid ) ;
    librados::bufferlist _dst1_bl = do_read( io_ctx, dst1_oid ) ;

    //do_remove( io_ctx, src_oid ) ;

    return 0 ;

  }

  // =========================================================== //
  // SPLIT 1

  int split1( librados::IoCtx io_ctx, std::string src_oid )
  {
    std::cout << "\n============\nrunning split1\n\n" << std::endl ;
    std::cout << "    src_oid      = " << src_oid << std::endl ;
    std::cout << "\n" << std::endl ;

    // write new empty objects
    std::string src_oid0 = src_oid + "_split10" ;
    std::string src_oid1 = src_oid + "_split11" ;
    const char* dst0_oid = src_oid0.c_str() ;
    const char* dst1_oid = src_oid1.c_str() ;

    std::cout << "------------- start -------------" << std::endl ;

    librados::bufferlist src_bl  = do_read( io_ctx, src_oid ) ;

    std::string data = (std::string)src_bl.c_str() ;
    int len          = data.size() ;
    int offset       = len / 2 ;

    std::cout << data << std::endl ;
    std::cout << len << std::endl ;
    std::cout << offset << std::endl ;

    librados::bufferlist dst0_data ;
    dst0_data.append( src_bl.c_str() + 0, offset ) ;
    do_write( io_ctx, dst0_oid, dst0_data ) ;

    librados::bufferlist dst1_data ;
    dst1_data.append( src_bl.c_str() + offset, len - offset ) ;
    do_write( io_ctx, dst1_oid, dst1_data ) ;

    std::cout << "------------- end -------------" << std::endl ;

    librados::bufferlist _src_bl  = do_read( io_ctx, src_oid ) ;
    librados::bufferlist _dst0_bl = do_read( io_ctx, dst0_oid ) ;
    librados::bufferlist _dst1_bl = do_read( io_ctx, dst1_oid ) ;

    //do_remove( io_ctx, src_oid ) ;

    return 0 ;
  }

  // =========================================================== //
  // COPY RANGE 2
  librados::bufferlist copy_range2( librados::IoCtx io_ctx, 
                                    std::string src_oid,
                                    std::string dst_oid,
                                    int start_off,
                                    int end_off )
  {
    std::cout << "------------- copy range 2 start -------------" << std::endl ;
    std::cout << "  src_oid   = " << src_oid << std::endl ;
    std::cout << "  dst_oid   = " << dst_oid << std::endl ;
    std::cout << "  start_off = " << start_off << std::endl ;
    std::cout << "  end_off   = " << end_off << std::endl ;

    librados::bufferlist src_bl  = do_read( io_ctx, src_oid ) ;

    librados::bufferlist dst_data ;
    src_bl.copy( start_off, end_off - start_off, dst_data ) ;
    dst_data.append( src_bl.c_str() + start_off, end_off - start_off ) ;
    do_write( io_ctx, dst_oid, dst_data ) ;

    std::cout << "------------- copy range 2 end -------------" << std::endl ;
    return dst_data ;
  }

  // =========================================================== //
  // COPY RANGE
  librados::bufferlist copy_range1( librados::IoCtx io_ctx, 
                                    std::string src_oid,
                                    std::string dst_oid,
                                    int start_off,
                                    int end_off )
  {
    std::cout << "------------- copy range start -------------" << std::endl ;

    librados::bufferlist src_bl  = do_read( io_ctx, src_oid ) ;

    librados::bufferlist dst_data ;
    dst_data.append( src_bl.c_str() + start_off, end_off - start_off ) ;
    do_write( io_ctx, dst_oid, dst_data ) ;

    std::cout << "------------- copy range end -------------" << std::endl ;
    return dst_data ;
  }


  // =========================================================== //
  // DO REMOVE
  void do_remove( librados::IoCtx io_ctx, std::string oid )
  {
    {
      int ret = io_ctx.remove( oid ) ;
      if (ret < 0) {
        std::cerr << "Couldn't remove object! error " << ret << std::endl;
        exit(EXIT_FAILURE);
      } else {
        std::cout << "Removed object '" << oid << "'" << std::endl;
      }
    }
  }


  // =========================================================== //
  // DO READ
  librados::bufferlist do_read( librados::IoCtx io_ctx, 
                                std::string obj_name )
  {
    librados::bufferlist read_buf;
    {

      //Create I/O Completion.
      librados::AioCompletion *read_completion = librados::Rados::aio_create_completion();

      //Send read request.
      int ret = io_ctx.aio_read(obj_name, read_completion, &read_buf, 0, 0);
      if (ret < 0) {
        std::cerr << "Couldn't start read object! error " << ret << std::endl;
        exit(EXIT_FAILURE);
      }

      // Wait for the request to complete, and check that it succeeded.
      read_completion->wait_for_complete();
      ret = read_completion->get_return_value();
      if (ret < 0) {
        std::cerr << "Couldn't read object! error " << ret << std::endl;
        exit(EXIT_FAILURE);
      } else {
        std::cout << "Read object '" << obj_name<< "':"  << std::endl;
        std::cout << read_buf.c_str() << std::endl;
      }
    }
    return read_buf ;
  }

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

  void do_write( librados::IoCtx io_ctx, 
                 std::string obj_name, 
                 librados::bufferlist obj_bl ) 
  {
    {
      int ret = io_ctx.write_full( obj_name, obj_bl ) ;
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
  // Needs improvement:
  //   0. assumes string data! need to generalize!
  //   1. do not take in-memory contents of the object! 
  //      wastes precious ram.
  //   2. pass the io_ctx, object, and byte offset only.

  // given contents of an object in memory, divide the object into two
  // separate objects and remove the original object
  void do_split( librados::IoCtx io_ctx, const char *obj_name, librados::bufferlist read_buf )
  {
    std::cout << "SplitUtils do_split()" << std::endl ;
    std::cout << "obj_name = " << obj_name << std::endl ;
    std::cout << "read_buf = " << read_buf.c_str() << std::endl ;

    // split data in memory
    // current crippling assumption: data is str
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
