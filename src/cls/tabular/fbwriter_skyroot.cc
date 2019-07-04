/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/

// bin/fbwriter_skyroot --oid atable_transposed --pool tpchflatbuf --filename blah.txt --debug yes

#include "include/types.h"
#include <string>
#include <sstream>
#include <type_traits>
#include <errno.h>
#include <time.h>
#include <boost/algorithm/string/classification.hpp> // for boost::is_any_of
#include <boost/algorithm/string/split.hpp> // for boost::split
#include <boost/algorithm/string.hpp> // for boost::trim
#include <boost/lexical_cast.hpp>
#include <boost/program_options.hpp>

#include "re2/re2.h"
#include "objclass/objclass.h"
#include <iostream>
#include <algorithm>
#include <atomic>
#include <thread>
#include <condition_variable>
#include "re2/re2.h"
#include "include/rados/librados.hpp"
#include "skyroot_generated.h"

namespace po = boost::program_options ;

void do_write( bool, 
               std::string,
               std::string,
               std::string,
               std::string ) ;

int main( int argc, char *argv[] ) {

  bool debug ;
  std::string oid  ;
  std::string pool ;
  std::string write_type ;
  std::string filename ;

  po::options_description gen_opts("General options");
  gen_opts.add_options()
    ("help,h", "show help message")
    ("debug", po::value<bool>(&debug)->required(), "debug")
    ("oid", po::value<std::string>(&oid)->required(), "oid")
    ("pool", po::value<std::string>(&pool)->required(), "pool")
    ("write_type", po::value<std::string>(&write_type)->required(), "write_type")
    ("filename", po::value<std::string>(&filename)->required(), "filename") ;

  po::options_description all_opts( "Allowed options" ) ;
  all_opts.add( gen_opts ) ;
  po::variables_map vm ;
  po::store( po::parse_command_line( argc, argv, all_opts ), vm ) ;
  if( vm.count( "help" ) ) {
    std::cout << all_opts << std::endl ;
    return 1;
  }
  po::notify( vm ) ;

  do_write( debug, 
            oid,
            pool,
            write_type, 
            filename ) ;

  return 0 ;
} // main

// =========== //
//   DO WRITE  //
// =========== //
void do_write( bool debug, 
               std::string oid,
               std::string pool,
               std::string write_type, 
               std::string filename ) {

  if( debug ) {
    std::cout << "debug      : " << debug      << std::endl ;
    std::cout << "oid        : " << oid        << std::endl ;
    std::cout << "pool       : " << pool       << std::endl ;
    std::cout << "write_type : " << write_type << std::endl ;
    std::cout << "filename   : " << filename   << std::endl ;
  }

  // -----------------------------------------------
  // | FBMeta flatbuffer     | Rows flatbuffer     |
  // -----------------------------------------------
  if( write_type == "rows" ) {

    flatbuffers::FlatBufferBuilder builder( 1024 ) ;
    librados::bufferlist bl_seq ;

    // --------------------------------------------- //
    // build fb meta bufferlist

    uint64_t nrows = 4 ;
    uint64_t ncols = 3 ;

    std::vector< uint8_t > meta_schema ;
    meta_schema.push_back( (uint8_t)0 ) ; // 0 --> uint64
    meta_schema.push_back( (uint8_t)1 ) ; // 1 --> float
    auto a = builder.CreateVector( meta_schema ) ;

    std::vector< uint64_t > rids_vect ;
    std::vector< uint64_t > int_vect_raw ;
    std::vector< float > float_vect_raw ;
    std::vector< flatbuffers::Offset<flatbuffers::String> > string_vect_raw ;

/*
    rids_vect.push_back( 1 ) ;
    rids_vect.push_back( 2 ) ;
    rids_vect.push_back( 3 ) ;
    rids_vect.push_back( 4 ) ;
    int_vect_raw.push_back( 10 ) ;
    int_vect_raw.push_back( 20 ) ;
    int_vect_raw.push_back( 30 ) ;
    int_vect_raw.push_back( 40 ) ;
    float_vect_raw.push_back( 11.5 ) ;
    float_vect_raw.push_back( 21.5 ) ;
    float_vect_raw.push_back( 31.5 ) ;
    float_vect_raw.push_back( 41.5 ) ;
    string_vect_raw.push_back( builder.CreateString( "aaaaaaa" ) ) ;
    string_vect_raw.push_back( builder.CreateString( "bbbb" ) ) ;
    string_vect_raw.push_back( builder.CreateString( "blahblahblahblah" ) ) ;
    string_vect_raw.push_back( builder.CreateString( "1234" ) ) ;

    std::vector< flatbuffers::Offset< flatbuffers::String > > schema ;
    schema.push_back( builder.CreateString( "att0" ) ) ;
    schema.push_back( builder.CreateString( "att1" ) ) ;
    schema.push_back( builder.CreateString( "att2" ) ) ;

    //place these before record_builder declare
    auto table_name     = builder.CreateString( "atable" ) ;
    auto layout         = builder.CreateString( "ROW" ) ;
    auto schema_fb      = builder.CreateVector( schema ) ;
    auto rids_vect_fb   = builder.CreateVector( rids_vect ) ;

    std::vector< uint8_t > record_data_type_vect ;
    record_data_type_vect.push_back( Tables::Data_IntData ) ;
    record_data_type_vect.push_back( Tables::Data_FloatData ) ;
    record_data_type_vect.push_back( Tables::Data_StringData ) ;
    auto record_data_types = builder.CreateVector( record_data_type_vect ) ;

    std::vector< flatbuffers::Offset< Tables::Record > > row_records ;

    // create Records as lists of Datas
    for( unsigned int i = 0; i < nrows; i++ ) {

      std::vector< uint64_t > single_iv ;
      std::vector< float > single_fv ;
      std::vector< flatbuffers::Offset<flatbuffers::String> > single_sv ;

      single_iv.push_back( int_vect_raw[ i ] ) ;
      single_fv.push_back( float_vect_raw[ i ] ) ;
      single_sv.push_back( string_vect_raw[ i ] ) ;

      auto int_vect_fb    = builder.CreateVector( single_iv ) ;
      auto float_vect_fb  = builder.CreateVector( single_fv ) ;
      auto string_vect_fb = builder.CreateVector( single_sv ) ;

      // data items
      auto iv = Tables::CreateIntData( builder, int_vect_fb ) ;
      auto fv = Tables::CreateFloatData( builder, float_vect_fb ) ;
      auto sv = Tables::CreateStringData( builder, string_vect_fb ) ;

      // record data
      std::vector< flatbuffers::Offset< void > > data_vect ;
      data_vect.push_back( iv.Union() ) ;
      data_vect.push_back( fv.Union() ) ;
      data_vect.push_back( sv.Union() ) ;
      auto data = builder.CreateVector( data_vect ) ;

      auto rec = Tables::CreateRecord( builder, record_data_types, data ) ;
      row_records.push_back( rec ) ;
    }

    auto row_records_fb = builder.CreateVector( row_records ) ;

    // create the Rows flatbuffer:
    auto rows = Tables::CreateRows(
      builder,
      0,
      0,
      table_name,
      schema_fb,
      nrows,
      ncols,
      layout,
      rids_vect_fb,
      row_records_fb ) ;

    Tables::RootBuilder root_builder( builder ) ;
    root_builder.add_schema( a ) ;

    // save the Rows flatbuffer to the root flatbuffer
    root_builder.add_relationData_type( Tables::Relation_Rows ) ;
    root_builder.add_relationData( rows.Union() ) ;

    auto res = root_builder.Finish() ;
    builder.Finish( res ) ;

    const char* fb = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;
    int bufsz      = builder.GetSize() ;
    librados::bufferlist bl ;
    bl.append( fb, bufsz ) ;

    // write to flatbuffer
    //librados::bufferlist bl_seq ;
    ::encode( bl, bl_seq ) ;

    // save to ceph object
    // connect to rados
    librados::Rados cluster;
    cluster.init(NULL);
    cluster.conf_read_file(NULL);
    int ret = cluster.connect();
    checkret(ret, 0);

    // open pool
    librados::IoCtx ioctx ;
    ret = cluster.ioctx_create( "tpchflatbuf" , ioctx ) ;
    checkret( ret, 0 ) ;

    // write bl_seq to ceph object
    const char *obj_name = oid.c_str() ;
    bufferlist::iterator p = bl_seq.begin();
    size_t i = p.get_remaining() ;
    std::cout << i << std::endl ;
    ret = ioctx.write( obj_name, bl_seq, i, 0 ) ;
    checkret(ret, 0);

    ioctx.close() ;
*/
  }

  // -----------------------------------------------
  // | FBMeta flatbuffer     | Col flatbuffer     |
  // -----------------------------------------------
  else if( write_type == "col" ) {
  }
  else {
    std::cout << ">> unrecognized write_type = '" << write_type << "'" << std::endl ;
  }
} // do_write
