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
#include "cls/tabular/cls_tabular_utils.h"

namespace po = boost::program_options;

int main(int argc, char **argv) {

  bool debug ;
  std::string pool;
  std::string project_cols;
  std::string data_schema;
  int start_oid;
  int end_oid;
  int merge_id;

  po::options_description gen_opts("General options");
  gen_opts.add_options()   ("help,h", "show help message")
    ("debug",        po::value<bool>(&debug)->required(), "debug")
    ("pool",        po::value<std::string>(&pool)->required(), "pool")
    ("project-cols", po::value<std::string>(&project_cols)->required(), "project columns")
    ("data-schema", po::value<std::string>(&data_schema)->required(), "data schema")
    ("start-oid",   po::value<int>(&start_oid)->required(),    "number for starting oid")
    ("end-oid",     po::value<int>(&end_oid)->required(),      "number for ending oid")
    ("merge-id",    po::value<int>(&merge_id)->required(),     "number id for merge object")
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

  std::string target_objname = "obj.client.mergetarget."+std::to_string(merge_id) ;
  int curr_offset = 0 ;
  for( int j=start_oid; j < (end_oid+1); j++ ) {
    std::string src_objname = "obj."+std::to_string(j) ;
    if ( debug ) {
      std::cout << src_objname << std::endl ;
      std::cout << target_objname << std::endl ;
    }

    // read src fbx object
    librados::bufferlist src_bl ;
    int num_bytes_read = ioctx.read( src_objname.c_str(), src_bl, (size_t)0, (uint64_t)0 ) ;
    if( debug ) std::cout << "num_bytes_read src : " << num_bytes_read << std::endl ;

    // transform into arrow
    boost::trim(project_cols);
    boost::to_upper(project_cols);

    auto sky_tbl_schema = Tables::schemaFromString(data_schema) ;
    auto sky_qry_schema = Tables::schemaFromColNames(sky_tbl_schema, project_cols) ;
    auto qop_query_schema = Tables::schemaToString(sky_qry_schema) ;
    Tables::schema_vec query_schema = Tables::schemaFromString(qop_query_schema) ;

    // ---------------------------------------- //
    ceph::bufferlist::iterator it = src_bl.begin();
    if ( debug ) std::cout << "it.get_remaining() = " << it.get_remaining() << std::endl ;
    while (it.get_remaining() > 0) {
      if ( debug ) std::cout << "it.get_remaining() = " << it.get_remaining() << std::endl ;
      bufferlist bl;
      bufferlist transformed_encoded_meta_bl;
      try {
          ::decode(bl, it);  // unpack the next bl
      } catch (const buffer::error &err) {
          if ( debug ) std::cout << "ERROR: decoding object format from BL" << std::endl ;
          return 1;
      }

      // default usage here assumes the fbmeta is already in the bl
      Tables::sky_meta meta = Tables::getSkyMeta(&bl);
      std::string errmsg;

      // CREATE An FB_META, start with an empty builder first
      flatbuffers::FlatBufferBuilder *meta_builder = new flatbuffers::FlatBufferBuilder();

      std::shared_ptr<arrow::Table> table;
      ret = Tables::transform_fb_to_arrow(meta.blob_data, meta.blob_size,
                                          query_schema, errmsg, &table);

      // Convert arrow to a buffer
      std::shared_ptr<arrow::Buffer> buffer;
      Tables::convert_arrow_to_buffer(table, &buffer);
      Tables::createFbMeta(meta_builder,
                           SFT_ARROW,
                           reinterpret_cast<unsigned char*>(buffer->mutable_data()),
                           buffer->size());

      // Add meta_builder's data into a bufferlist as char*
      bufferlist meta_bl;
      meta_bl.append(reinterpret_cast<const char*>(                   \
                     meta_builder->GetBufferPointer()),
                     meta_builder->GetSize());
      ::encode(meta_bl, transformed_encoded_meta_bl);
      delete meta_builder;

      // write arrow object
      if ( debug ) std::cout << "num bytes written to target : " << meta_bl.length() << std::endl ;
      ret = ioctx.write( target_objname, src_bl, meta_bl.length(), (size_t)curr_offset ) ;
      curr_offset += meta_bl.length() ;
      assert(ret == 0);
    } // end while
    // ---------------------------------------- //
  } // for loop

  ioctx.close();
  return 0 ;
}
