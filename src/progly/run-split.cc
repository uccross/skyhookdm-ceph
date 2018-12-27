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
#include "cls/tabular/cls_split_utils.h"
#include "cls/tabular/cls_tabular_utils.h"

namespace po = boost::program_options;

int main(int argc, char **argv)
{

  std::cout << "let's start splittin!!!" << std::endl ;

  // after placing flatbuffers in rados:
  // 1. list all rados objects.
  // 2. get all data from a flatbuffer
  // 3. divide the data into two
  // 4. create two new ceph objects and store in rados
  // 5. delete the original object
  // 6. read in the data for all three objects again

  // ---------------------------------------------------------------------------- //

  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  checkret(ret, 0);

  /* Continued from previous C++ example, where cluster handle and
   * connection are established. First declare an I/O Context.
   */

  librados::IoCtx io_ctx;
  const char *pool_name = "tpchflatbuf";
  const char *obj_name  = "split1";
  std::string obj_data  = "string_with_odd_num_chars0" ;

  {
    ret = cluster.ioctx_create(pool_name, io_ctx);
    if (ret < 0) {
      std::cerr << "Couldn't set up ioctx! error " << ret << std::endl;
      exit(EXIT_FAILURE);
    } else {
      std::cout << "Created an ioctx for the pool." << std::endl;
    }
  }


  /* Write an object synchronously. */
  {
    librados::bufferlist bl;
    bl.append( obj_data );
    ret = io_ctx.write_full(obj_name, bl);
    if (ret < 0) {
      std::cerr << "Couldn't write object! error " << ret << std::endl;
      exit(EXIT_FAILURE);
    } else {
      std::cout << "Wrote new object '"  << obj_name << "' " << std::endl;
    }
  }


  ///*
  // * Add an xattr to the object.
  // */
  //{
  //  librados::bufferlist lang_bl;
  //  lang_bl.append("en_US");
  //  ret = io_ctx.setxattr("hw", "lang", lang_bl);
  //  if (ret < 0) {
  //    std::cerr << "failed to set xattr version entry! error "
  //    << ret << std::endl;
  //    exit(EXIT_FAILURE);
  //  } else {
  //    std::cout << "Set the xattr 'lang' on our object!" << std::endl;
  //  }
  //}


  /*
   * Read the object back asynchronously.
   */
  {
    librados::bufferlist read_buf;
    int read_len = 4194304;

    //Create I/O Completion.
    librados::AioCompletion *read_completion = librados::Rados::aio_create_completion();

    //Send read request.
    ret = io_ctx.aio_read(obj_name, read_completion, &read_buf, read_len, 0);
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
      std::cout << "Read object '" << obj_name << "' asynchronously with contents:" << std::endl;
      std::cout << read_buf.c_str() << std::endl;
    }

    SplitUtils::do_split( io_ctx, obj_name, read_buf ) ;

  }

//  /*
//   * Read the xattr.
//   */
//  {
//    librados::bufferlist lang_res;
//    ret = io_ctx.getxattr("hw", "lang", lang_res);
//    if (ret < 0) {
//      std::cerr << "failed to get xattr version entry! error "
//      << ret << std::endl;
//      exit(EXIT_FAILURE);
//    } else {
//      std::cout << "Got the xattr 'lang' from object hw!"
//      << lang_res.c_str() << std::endl;
//    }
//  }
//
//
//  /*
//   * Remove the xattr.
//   */
//  {
//    ret = io_ctx.rmxattr("hw", "lang");
//    if (ret < 0) {
//      std::cerr << "Failed to remove xattr! error "
//      << ret << std::endl;
//      exit(EXIT_FAILURE);
//    } else {
//      std::cout << "Removed the xattr 'lang' from our object!" << std::endl;
//    }
//  }
//
//  /*
//   * Remove the object.
//   */
//  {
//    ret = io_ctx.remove("hw");
//    if (ret < 0) {
//      std::cerr << "Couldn't remove object! error " << ret << std::endl;
//      exit(EXIT_FAILURE);
//    } else {
//      std::cout << "Removed object 'hw'." << std::endl;
//    }
//  }

  // ---------------------------------------------------------------------------- //

  //// connect to rados
  //librados::Rados cluster;
  //cluster.init(NULL);
  //cluster.conf_read_file(NULL);
  //int ret = cluster.connect();
  //checkret(ret, 0);

  //// open pool
  //librados::IoCtx ioctx;
  //ret = cluster.ioctx_create("tpchflatbuf", ioctx);
  //checkret(ret, 0);

  //// build an aiostate
  //AioState *s = new AioState;
  //s->c = librados::Rados::aio_create_completion( s, NULL, handle_cb);
  //memset(&s->times, 0, sizeof(s->times));
  //s->times.dispatch = getns();

  //// do the read???
  //const std::string oid = "obj.0" ;
  //ret = ioctx.aio_read(oid, s->c, &s->bl, 0, 0);
  //checkret(ret, 0);

  //ceph::bufferlist wrapped_bls = s->bl;  // contains a seq of encoded bls.

  //std::cout << "blah" << std::endl ; 

  //// decode and process each bl (contains 1 flatbuf) in a loop.
  //ceph::bufferlist::iterator it = wrapped_bls.begin();
  //std::cout << it.get_remaining() << std::endl ;
  //while (it.get_remaining() > 0) {

  //  std::cout << "asdf" << std::endl ;

  //  //ceph::bufferlist bl;
  //  //::decode(bl, it);  // unpack the next bl (flatbuf)

  //  //// get our data as contiguous bytes before accessing as flatbuf
  //  //const char* fb = bl.c_str();
  //  //size_t fb_size = bl.length();
  //  //Tables::sky_root root = Tables::getSkyRoot(fb, fb_size);

  //  //// local counter to accumulate nrows in all flatbuffers received.
  //  //rows_returned += root.nrows;

  //}

  //delete s ;
  //ioctx.close();

  // ---------------------------------------------------------------------------- //

  //std::list<AioState*> ready_ios;

  //// connect to rados
  //std::string pool = "tpchflatbuf" ;
  //librados::Rados cluster;
  //cluster.init(NULL);
  //cluster.conf_read_file(NULL);
  //int ret0 = cluster.connect();
  //checkret(ret0, 0);

  //librados::IoCtx ioctx;
  //int ret1 = cluster.ioctx_create(pool.c_str(), ioctx);
  //checkret(ret1, 0);

  //std::vector<std::thread> threads;
  //threads.push_back(std::thread(worker));

  //std::condition_variable dispatch_cond;
  //std::unique_lock<std::mutex> lock(dispatch_lock);
  //lock.unlock();

  //AioState *s = new AioState;
  //// gets an aio_completion
  //// s => a blank aiostate
  //// NULL => rados callback_t
  //// handle_cb => function for firing up a worker
  //s->c = librados::Rados::aio_create_completion( s, NULL, handle_cb);
  //memset(&s->times, 0, sizeof(s->times));
  //s->times.dispatch = getns();
  //const std::string oid = "oid.0" ;

  //int ret2 = ioctx.aio_read(oid, s->c, &s->bl, 0, 0);
  //checkret(ret2, 0);

  //lock.lock() ;
  //dispatch_cond.wait(lock);
  //lock.unlock() ;

  //for (auto& thread : threads) {
  //  thread.join();
  //}

  //delete s ;
  //ioctx.close();

  // ---------------------------------------------------------------------------- //
  // play with accessing flatbuffer data through flatbuffer api:
  // get number of rows per flatbuffer
  // divide rows into 3+ ceph objects.
  ////flatbuffers::FlatBufferBuilder flatbuffer_builder(1024) ; // pre-alloc size
  //// https://google.github.io/flatbuffers/flatbuffers_guide_use_cpp.html
  //// opening flatbuffer file:
  //std::ifstream infile ;
  //infile.open( argv[1], std::ios::binary | std::ios::in ) ;
  //infile.seekg( 0, std::ios::end ) ;
  //int length = infile.tellg() ;
  //std::cout << length << std::endl ;
  //infile.seekg( 0, std::ios::beg ) ;
  //char *data = new char[length] ;
  //infile.read( data, length ) ;
  //infile.close();
  //const Tables::Table* data_table = Tables::GetTable(data);
  //std::cout << data_table->nrows() << std::endl; 
  //std::cout << data_table->skyhook_version() << std::endl ;
  ////std::cout << data_table->schema_version() << std::endl ;
  ////std::cout << data_table->table_name()->str() << std::endl ;
  ////std::cout << data_table->schema()->str() << std::endl ;
  ////std::cout << data_table->rows() << std::endl ;
  //bufferlist bl;
  //bl.append("ABC", 3);
  //bufferlist::iterator i(&bl);
  //std::cout << "*i = " << *i << std::endl ;
  //++i;
  //std::cout << "*i = " << *i << std::endl ;
  //++i ;
  //std::cout << "*i = " << *i << std::endl ;
  //// This must be called after `Finish()`.
  ////uint8_t *buf = flatbuffer_builder.GetBufferPointer();
  ////int size = flatbuffer_builder.GetSize(); // Returns the size of the buffer that
  //                                         // `GetBufferPointer()` points to.

  ////std::cout << "*buf " << *buf << std::endl ;
  ////std::cout << "size " << size << std::endl ;

  std::cout << "phew! done!" << std::endl ;
  return 0 ;
}
