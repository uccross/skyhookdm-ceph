/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#include "include/rados/librados.hpp" // for librados
#include <boost/algorithm/string.hpp> // for boost::trim
#include <boost/algorithm/string/join.hpp>
#include <boost/program_options.hpp>
#include <iostream>

namespace po = boost::program_options;

int main(int argc, char **argv) {

  std::string pool;
  int start_oid;
  int end_oid;
  int merge_id;

  po::options_description gen_opts("General options");
  gen_opts.add_options()
    ("help,h", "show help message")
    ("pool",      po::value<std::string>(&pool)->required(), "pool")
    ("start-oid", po::value<int>(&start_oid)->required(),    "number for starting oid")
    ("end-oid",   po::value<int>(&end_oid)->required(),      "number for ending oid")
    ("merge-id",  po::value<int>(&merge_id)->required(),     "number id for merge object")
 ;

  po::options_description all_opts( "Allowed options" ) ;
  all_opts.add( gen_opts ) ;
  po::variables_map vm ;
  po::store( po::parse_command_line( argc, argv, all_opts ), vm ) ;
  if( vm.count( "help" ) ) {
    std::cout << all_opts << std::endl ;
    return 1;
  }
  po::notify( vm ) ;

  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  assert(ret == 0);

  // open pool
  librados::IoCtx ioctx;
  ret = cluster.ioctx_create(pool.c_str(), ioctx);
  assert(ret == 0);

  for( int j=start_oid; j < end_oid; j++ ) {
    std::string target_objname = "obj.client.mergetarget."+std::to_string(merge_id) ;
    std::string src_objname = "obj."+std::to_string(j) ;
    //std::cout << src_objname << std::endl ;
    //std::cout << target_objname << std::endl ;

    // read src object
    librados::bufferlist src_bl ;
    int num_bytes_read = ioctx.read( src_objname.c_str(), src_bl, (size_t)0, (uint64_t)0 ) ;
    //std::cout << "num_bytes_read src : " << num_bytes_read << std::endl ;

    // write target target object
    //std::cout << "num bytes written to target : " << src_bl.length() << std::endl ;
    ret = ioctx.write( target_objname, src_bl, src_bl.length(), 0 ) ;
    assert(ret == 0);
  }

  ioctx.close();
  return 0 ;
}
