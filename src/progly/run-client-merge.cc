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
/*
    std::string data_schema = ""
      "0 8 1 0 att0; " +
      "1 8 0 0 att1; " +
      "2 8 0 0 att2; " +
      "3 8 0 0 att3; " +
      "4 8 0 0 att4; " +
      "5 8 0 0 att5; " +
      "6 8 0 0 att6; " +
      "7 8 0 0 att7; " +
      "8 8 0 0 att8; " +
      "9 8 0 0 att9; " +
      "10 8 0 0 att10; " +
      "11 8 0 0 att11; " +
      "12 8 0 0 att12; " +
      "13 8 0 0 att13; " +
      "14 8 0 0 att14; " +
      "15 8 0 0 att15; " +
      "16 8 0 0 att16; " +
      "17 8 0 0 att17; " +
      "18 8 0 0 att18; " +
      "19 8 0 0 att19; " +
      "20 8 0 0 att20; " +
      "21 8 0 0 att21; " +
      "22 8 0 0 att22; " +
      "23 8 0 0 att23; " +
      "24 8 0 0 att24; " +
      "25 8 0 0 att25; " +
      "26 8 0 0 att26; " +
      "27 8 0 0 att27; " +
      "28 8 0 0 att28; " +
      "29 8 0 0 att29; " +
      "30 8 0 0 att30; " +
      "31 8 0 0 att31; " +
      "32 8 0 0 att32; " +
      "33 8 0 0 att33; " +
      "34 8 0 0 att34; " +
      "35 8 0 0 att35; " +
      "36 8 0 0 att36; " +
      "37 8 0 0 att37; " +
      "38 8 0 0 att38; " +
      "39 8 0 0 att39; " +
      "40 8 0 0 att40; " +
      "41 8 0 0 att41; " +
      "42 8 0 0 att42; " +
      "43 8 0 0 att43; " +
      "44 8 0 0 att44; " +
      "45 8 0 0 att45; " +
      "46 8 0 0 att46; " +
      "47 8 0 0 att47; " +
      "48 8 0 0 att48; " +
      "49 8 0 0 att49; " +
      "50 8 0 0 att50; " +
      "51 8 0 0 att51; " +
      "52 8 0 0 att52; " +
      "53 8 0 0 att53; " +
      "54 8 0 0 att54; " +
      "55 8 0 0 att55; " +
      "56 8 0 0 att56; " +
      "57 8 0 0 att57; " +
      "58 8 0 0 att58; " +
      "59 8 0 0 att59; " +
      "60 8 0 0 att60; " +
      "61 8 0 0 att61; " +
      "62 8 0 0 att62; " +
      "63 8 0 0 att63; " +
      "64 8 0 0 att64; " +
      "65 8 0 0 att65; " +
      "66 8 0 0 att66; " +
      "67 8 0 0 att67; " +
      "68 8 0 0 att68; " +
      "69 8 0 0 att69; " +
      "70 8 0 0 att70; " +
      "71 8 0 0 att71; " +
      "72 8 0 0 att72; " +
      "73 8 0 0 att73; " +
      "74 8 0 0 att74; " +
      "75 8 0 0 att75; " +
      "76 8 0 0 att76; " +
      "77 8 0 0 att77; " +
      "78 8 0 0 att78; " +
      "79 8 0 0 att79; " +
      "80 8 1 0 att80; " +
      "81 8 0 0 att81; " +
      "82 8 0 0 att82; " +
      "83 8 0 0 att83; " +
      "84 8 0 0 att84; " +
      "85 8 0 0 att85; " +
      "86 8 0 0 att86; " +
      "87 8 0 0 att87; " +
      "88 8 0 0 att88; " +
      "89 8 0 0 att89; " +
      "90 8 0 0 att90; " +
      "91 8 0 0 att91; " +
      "92 8 0 0 att92; " +
      "93 8 0 0 att93; " +
      "94 8 0 0 att94; " +
      "95 8 0 0 att95; " +
      "96 8 0 0 att96; " +
      "97 8 0 0 att97; " +
      "98 8 0 0 att98; " +
      "99 8 0 0 att99"
*/
    std::string data_schema = 
      "0 3 1 0 ORDERKEY; " +
      "1 3 0 1 PARTKEY; " +
      "2 3 0 1 SUPPKEY; " +
      "3 3 1 0 LINENUMBER; " +
      "4 12 0 1 QUANTITY; " +
      "5 13 0 1 EXTENDEDPRICE; " +
      "6 12 0 1 DISCOUNT; " +
      "7 13 0 1 TAX; " +
      "8 9 0 1 RETURNFLAG; " +
      "9 9 0 1 LINESTATUS; " +
      "10 14 0 1 SHIPDATE; " +
      "11 14 0 1 COMMITDATE; " +
      "12 14 0 1 RECEIPTDATE; " +
      "13 15 0 1 SHIPINSTRUCT; " +
      "14 15 0 1 SHIPMODE; " +
      "15 15 0 1 COMMENT"
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

  std::string target_objname = "obj.client.mergetarget."+std::to_string(merge_id) ;
  int curr_offset = 0 ;
  for( int j=start_oid; j < (end_oid+1); j++ ) {
    std::string src_objname = "obj."+std::to_string(j) ;
    std::cout << src_objname << std::endl ;
    std::cout << target_objname << std::endl ;

    // read src fbx object
    librados::bufferlist src_bl ;
    int num_bytes_read = ioctx.read( src_objname.c_str(), src_bl, (size_t)0, (uint64_t)0 ) ;
    std::cout << "num_bytes_read src : " << num_bytes_read << std::endl ;

    // transform into arrow
    std::string project_cols = "att0" ;
    boost::trim(project_cols);
    boost::to_upper(project_cols);

    auto sky_tbl_schema = Tables::schemaFromString(data_schema) ;
    auto sky_qry_schema = Tables::schemaFromColNames(sky_tbl_schema, project_cols) ;
    auto qop_query_schema = Tables::schemaToString(sky_qry_schema) ;
    Tables::schema_vec query_schema = Tables::schemaFromString(qop_query_schema) ;

    // ---------------------------------------- //
    ceph::bufferlist::iterator it = src_bl.begin();
    std::cout << "it.get_remaining() = " << it.get_remaining() << std::endl ;
    while (it.get_remaining() > 0) {
      std::cout << "it.get_remaining() = " << it.get_remaining() << std::endl ;
      bufferlist bl;
      bufferlist transformed_encoded_meta_bl;
      try {
          ::decode(bl, it);  // unpack the next bl
      } catch (const buffer::error &err) {
          std::cout << "ERROR: decoding object format from BL" << std::endl ;
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
      std::cout << "num bytes written to target : " << meta_bl.length() << std::endl ;
      ret = ioctx.write( target_objname, src_bl, meta_bl.length(), (size_t)curr_offset ) ;
      curr_offset += meta_bl.length() ;
      assert(ret == 0);
    } // end while
    // ---------------------------------------- //
  } // for loop

  ioctx.close();
  return 0 ;
}
