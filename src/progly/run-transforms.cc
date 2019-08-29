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
#include "cls/tabular/cls_transforms_utils.h"

#include <boost/program_options.hpp>

namespace po = boost::program_options;

int main(int argc, char **argv) {

  std::string pool ;
  std::string src_oid ;
  bool use_cls ;

  po::options_description gen_opts("General options");
  gen_opts.add_options()
    ("help,h", "show help message")
    ("pool", po::value<std::string>(&pool)->required(), "pool")
    ("src-oid", po::value<std::string>(&src_oid)->required(), "name of source object to transform")
    ("use-cls", po::value<bool>(&use_cls)->required(), "use cls?")
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

  std::cout << "pool    = " << pool << std::endl ;
  std::cout << "src-oid = " << src_oid << std::endl ;
  std::cout << "use-cls = " << use_cls << std::endl ;

  // --------------------------------- //
  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  checkret( ret, 0 ) ;
  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( pool.c_str(), ioctx ) ;
  checkret( ret, 0 ) ;
  // read bl_seq
  librados::bufferlist wrapped_bl_seq ;
  int num_bytes_read = ioctx.read( src_oid.c_str(), wrapped_bl_seq, (size_t)0, (uint64_t)0 ) ;
  std::cout << "num_bytes_read : " << num_bytes_read << std::endl ;
  // --------------------------------- //

  // extract FB_Meta. wrapped_bl_seq will only ever contain 1 bl, which is an FB_Meta.
  ceph::bufferlist::iterator it_wrapped = wrapped_bl_seq.begin() ;
  ceph::bufferlist meta_wrapper_bl ;
  ::decode( meta_wrapper_bl, it_wrapped ) ; // this decrements get_remaining by moving iterator
  //const char* meta_dataptr = meta_wrapper_bl.c_str() ;
  size_t meta_datasz       = meta_wrapper_bl.length() ;
  std::cout << "meta_datasz = " << meta_datasz << std::endl ;

  // get the blob
  const Tables::FB_Meta* meta = Tables::GetFB_Meta( meta_wrapper_bl.c_str() ) ;
  const char* blob_dataptr    = reinterpret_cast<const char*>( meta->blob_data()->Data() ) ;
  size_t blob_sz              = meta->blob_data()->size() ;
  std::cout << "blob_sz = " << blob_sz << std::endl ;

  //TODO: use different method for getting bufferlist from ptr.
  //  fb char* data ptr and len
  //  const char *c = fb->GetBuffer();
  //  int size = fb->GetBufferSize();
  //
  //  // standard bl.append our char* array
  //  bufferlist bl1;
  //  bl1.append(c, size);
  //
  //  // create bufptr and push ptr onto end of bl
  //  bufferptr p(c, size);
  //  bufferlist bl2;
  //  bl2.push_back(p);
  //
  //  // iterator with copy_shallow
  //  bufferptr p(c, size);
  //  bufferlist bl3;
  //  iterator it(&bl3);
  //  it.copy_shallow(size,p);

  ceph::bufferlist bl_seq ;
  bl_seq.append( blob_dataptr, blob_sz ) ;
  ceph::bufferlist::iterator it_bl_seq = bl_seq.begin() ;
  while( it_bl_seq.get_remaining() > 0 ) {
    std::cout << "it_bl_seq.get_remaining() = " << it_bl_seq.get_remaining() << std::endl ;

    ceph::bufferlist bl ;
    ::decode( bl, it_bl_seq ) ; // this decrements get_remaining by moving iterator
    const char* dataptr = bl.c_str() ;
    size_t datasz       = bl.length() ;
    std::cout << "datasz = " << datasz << std::endl ;

    //auto ret = Tables::printFlatbufFBUColAsCsv(
    //                  dataptr,
    //                  datasz,
    //                  print_header,
    //                  print_verbose,
    //                  max_to_print);
    //std::cout << ret << std::endl ;

    std::string errmsg = "" ;
    flatbuffers::FlatBufferBuilder transform_builder(1024);
    std::vector<int> project_cols{2} ;
    std::vector<std::string> project_cols_str{"2"} ;

    transform_args targs ;
    targs.project_cols = boost::algorithm::join(project_cols_str, ",");

    if(!use_cls) {
      auto ret = Tables::transform_fburows_to_fbucols(
                           dataptr,
                           datasz,
                           errmsg,
                           transform_builder,
                           project_cols);
      std::cout << ret << std::endl ;
    }
    else {
        //int ret = ioctx.aio_exec(oid, s->c,
        //    "tabular", "exec_query_op", inbl, &s->bl);
        //checkret(ret, 0);
    }

    std::cout << "loop while" << std::endl ;
  } // while

  return 0 ;
}
