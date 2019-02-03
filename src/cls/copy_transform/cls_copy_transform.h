/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#ifndef CLS_COPY_TRANSFORM_H
#define CLS_COPY_TRANSFORM_H

#include <string>

#include "include/rados/librados.hpp"

// https://stackoverflow.com/questions/3247861/example-of-uuid-generation-using-boost-in-c
#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.
#include <boost/lexical_cast.hpp>
#include <tuple>

namespace CopyTransform {

  void test_func()
  {
    std::cout << "executed CopyTransform func" << std::endl ;
  }

  // =========================================================== //
  // PROTOTYPES

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
  // COPY TRANSFORM

  int copy_transform( librados::IoCtx io_ctx, 
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

}

#endif
