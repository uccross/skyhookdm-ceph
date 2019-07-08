/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/

// bin/fbwriter_skyroot --oid atable_transposed --pool tpchflatbuf --filename ~/blah.txt --write_type rows --debug yes --schema_datatypes int,float,string --schema_attnames att0,att1,att2 --table_name atable --layout ROWS --nrows 4 --ncols 3

#include "include/types.h"
#include <string>
#include <sstream>
#include <fstream>
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

struct filedata_t {
  std::vector< std::vector< uint64_t  > > listof_int_vect_raw ;
  std::vector< std::vector< float > > listof_float_vect_raw ;
  std::vector< std::vector< std::string > > listof_string_vect_raw_strs ;
  std::vector< std::vector< flatbuffers::Offset<flatbuffers::String> > > listof_string_vect_raw ;
  std::vector< std::vector< std::string > > indexer ;
  uint64_t int_vect_cnt ;
  uint64_t float_vect_cnt ;
  uint64_t string_vect_cnt ;

  filedata_t() {
    // collect and store filedata
    // initialize struct with empty vects
    int_vect_cnt = 0 ;
    float_vect_cnt = 0 ;
    string_vect_cnt = 0 ;
  } ;

  void toString() {
    std::cout << "=============================================" << std::endl ;
    std::cout << "listof_int_vect_raw.size() = " << listof_int_vect_raw.size() << std::endl ;
    std::cout << "listof_int_vect_raw :" << std::endl ;
    std::cout << "----------------------" << std::endl ;
    for( int i = 0; i < listof_int_vect_raw.size(); i++ ) {
      std::cout << "listof_int_vect_raw[" << std::to_string( i ) << "].size() = " 
                << listof_int_vect_raw[i].size() << std::endl ;
      for( int j = 0; j < listof_int_vect_raw[i].size(); j++ ) {
        std::cout << listof_int_vect_raw[i][j] ;
        if( j < listof_int_vect_raw[i].size()-1 )
          std::cout << "," ;
      }
      std::cout << std::endl ;
    }
    std::cout << "=============================================" << std::endl ;
    std::cout << "listof_float_vect_raw.size() = " << listof_float_vect_raw.size() << std::endl ;
    std::cout << "listof_float_vect_raw :" << std::endl ;
    std::cout << "----------------------" << std::endl ;
    for( int i = 0; i < listof_float_vect_raw.size(); i++ ) {
      std::cout << "listof_float_vect_raw[" << std::to_string( i ) << "].size() = " 
                << listof_float_vect_raw[i].size() << std::endl ;
      for( int j = 0; j < listof_float_vect_raw[i].size(); j++ ) {
        std::cout << listof_float_vect_raw[i][j] ;
        if( j < listof_float_vect_raw[i].size()-1 )
          std::cout << "," ;
      }
      std::cout << std::endl ;
    }
    std::cout << "=============================================" << std::endl ;
    std::cout << "listof_string_vect_raw.size() = " << listof_string_vect_raw.size() << std::endl ;
    std::cout << "listof_string_vect_raw :" << std::endl ;
    std::cout << "----------------------" << std::endl ;
    for( int i = 0; i < listof_string_vect_raw.size(); i++ ) {
      std::cout << "listof_string_vect_raw[" << std::to_string( i ) << "].size() = " 
                << listof_string_vect_raw[i].size() << std::endl ;
      for( int j = 0; j < listof_string_vect_raw[i].size(); j++ ) {
        std::cout << listof_string_vect_raw_strs[i][j] ;
        if( j < listof_string_vect_raw[i].size()-1 )
          std::cout << "," ;
      }
      std::cout << std::endl ;
    }
  } // toString

  uint64_t get_index( std::string in_key ) {
    for( int i = 0; i < indexer.size(); i++ ) {
      std::string key   = indexer[i][0] ;
      std::string index = indexer[i][1] ;
      if( in_key == key) {
        return std::stoi( index ) ;
      }
    }
  } // get_index
} ;

struct cmdline_inputs_t {
  bool debug ;
  std::string oid  ;
  std::string pool ;
  std::string write_type ;
  std::string filename ;
  std::string schema_datatypes ;
  std::string schema_attnames ;
  std::string table_name ;
  std::string layout ;
  uint64_t nrows ;
  uint64_t ncols ;
  std::string writeto ;
  std::string targetoid ;
  std::string targetpool ;
} ;

int writeToDisk( librados::bufferlist wrapper_bl, int bufsz, std::string target_oid ) {

  int mode = 0600 ;
  std::string fname = "Skyhook.v2."+ target_oid ;
  wrapper_bl.write_file( fname.c_str(), mode ) ;
  printf( "buff size: %d, wrapper_bl size: %d\n", bufsz, wrapper_bl.length() ) ;

  return 0;
}

int writeToCeph( librados::bufferlist bl_seq, int bufsz, std::string target_oid, std::string target_pool ) {

  // save to ceph object
  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();

  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( target_pool.c_str(), ioctx ) ;

  // write bl_seq to ceph object
  const char *obj_name = target_oid.c_str() ;
  bufferlist::iterator p = bl_seq.begin();
  size_t i = p.get_remaining() ;
  std::cout << i << std::endl ;
  ret = ioctx.write( obj_name, bl_seq, i, 0 ) ;

  ioctx.close() ;
  return 0 ;
}

void do_write( cmdline_inputs_t ) ;

std::vector< std::string > parse_csv_str( std::string instr ) {
  std::stringstream ss( instr ) ;
  std::vector< std::string > result ;
  while( ss.good() ) {
      std::string substr ;
      getline( ss, substr, ',' ) ;
      result.push_back( substr ) ;
  }
  return result ;
}

std::vector< std::string > parse_psv_str( std::string instr ) {
  std::stringstream ss( instr ) ;
  std::vector< std::string > result ;
  while( ss.good() ) {
      std::string substr ;
      getline( ss, substr, '|' ) ;
      result.push_back( substr ) ;
  }
  return result ;
}



int main( int argc, char *argv[] ) {

  bool debug ;
  std::string oid  ;
  std::string pool ;
  std::string write_type ;
  std::string filename ;
  std::string schema_datatypes ;
  std::string schema_attnames ;
  std::string table_name ;
  std::string layout ;
  uint64_t nrows ;
  uint64_t ncols ;
  std::string writeto ;
  std::string targetoid ;
  std::string targetpool ;

  po::options_description gen_opts("General options");
  gen_opts.add_options()
    ("help,h", "show help message")
    ("debug", po::value<bool>(&debug)->required(), "debug")
    ("oid", po::value<std::string>(&oid)->required(), "oid")
    ("pool", po::value<std::string>(&pool)->required(), "pool")
    ("write_type", po::value<std::string>(&write_type)->required(), "write_type")
    ("filename", po::value<std::string>(&filename)->required(), "filename")
    ("schema_datatypes", po::value<std::string>(&schema_datatypes)->required(), "schema_datatypes")
    ("schema_attnames", po::value<std::string>(&schema_attnames)->required(), "schema_attnames")
    ("table_name", po::value<std::string>(&table_name)->required(), "table_name")
    ("layout", po::value<std::string>(&layout)->required(), "layout")
    ("nrows", po::value<uint64_t>(&nrows)->required(), "nrows")
    ("ncols", po::value<uint64_t>(&ncols)->required(), "ncols")
    ("writeto", po::value<std::string>(&writeto)->required(), "writeto")
    ("targetoid", po::value<std::string>(&targetoid)->required(), "targetoid")
    ("targetpool", po::value<std::string>(&targetpool)->required(), "targetpool") ;

  po::options_description all_opts( "Allowed options" ) ;
  all_opts.add( gen_opts ) ;
  po::variables_map vm ;
  po::store( po::parse_command_line( argc, argv, all_opts ), vm ) ;
  if( vm.count( "help" ) ) {
    std::cout << all_opts << std::endl ;
    return 1;
  }
  po::notify( vm ) ;

  cmdline_inputs_t inputs ;
  inputs.debug = debug ;
  inputs.oid = oid ;
  inputs.pool = pool ;
  inputs.write_type = write_type ;
  inputs.filename = filename ;
  inputs.schema_datatypes = schema_datatypes ;
  inputs.schema_attnames = schema_attnames ;
  inputs.table_name = table_name ;
  inputs.layout = layout ;
  inputs.nrows = nrows ;
  inputs.ncols = ncols ;
  inputs.writeto = writeto ;
  inputs.targetoid = targetoid ;
  inputs.targetpool = targetpool ;

  do_write( inputs ) ;

  return 0 ;
} // main

// =========== //
//   DO WRITE  //
// =========== //
void do_write( cmdline_inputs_t inputs ) {

  if( inputs.debug ) {
    std::cout << "inputs.debug            : " << inputs.debug                   << std::endl ;
    std::cout << "inputs.oid              : " << inputs.oid                     << std::endl ;
    std::cout << "inputs.pool             : " << inputs.pool                    << std::endl ;
    std::cout << "inputs.write_type       : " << inputs.write_type              << std::endl ;
    std::cout << "inputs.filename         : " << inputs.filename                << std::endl ;
    std::cout << "inputs.schema_datatypes : " << inputs.schema_datatypes        << std::endl ;
    std::cout << "inputs.schema_attnames  : " << inputs.schema_attnames         << std::endl ;
    std::cout << "inputs.table_name       : " << inputs.table_name              << std::endl ;
    std::cout << "inputs.layout           : " << inputs.layout                  << std::endl ;
    std::cout << "inputs.nrows            : " << std::to_string( inputs.nrows ) << std::endl ;
    std::cout << "inputs.ncols            : " << std::to_string( inputs.ncols ) << std::endl ;
  }

  // -----------------------------------------------
  // flatbuffer prelims
  // -----------------------------------------------
  flatbuffers::FlatBufferBuilder builder( 1024 ) ;
  librados::bufferlist bl_seq ;

  // -----------------------------------------------
  // | FBMeta flatbuffer     | Rows flatbuffer     |
  // -----------------------------------------------
  if( inputs.write_type == "rows" ) {

    // --------------------------------------------- //
    // --------------------------------------------- //
    uint64_t nrows = inputs.nrows ;
    uint64_t ncols = inputs.ncols ;

    // from commandline
    // place these before record_builder declare
    auto table_name     = builder.CreateString( inputs.table_name ) ;
    auto layout         = builder.CreateString( inputs.write_type ) ;

    // parse schema csv strings
    std::vector< std::string > schema_attnames = parse_csv_str( inputs.schema_attnames ) ;
    std::vector< flatbuffers::Offset< flatbuffers::String > > schema ;
    for( int i = 0; i < schema_attnames.size(); i++ ) {
      schema.push_back( builder.CreateString( schema_attnames[i] ) ) ;
    }
    auto schema_fb = builder.CreateVector( schema ) ;

    std::vector< std::string > schema_datatypes = parse_csv_str( inputs.schema_datatypes ) ;
    std::vector< uint8_t > record_data_type_vect ;
    for( int i = 0; i < schema_datatypes.size(); i++ ) {
      std::string this_dt = schema_datatypes[i] ;
      if( this_dt == "int" )
        record_data_type_vect.push_back( Tables::Data_IntData ) ;
      else if( this_dt == "float" )
        record_data_type_vect.push_back( Tables::Data_FloatData ) ;
      else if( this_dt == "string" )
        record_data_type_vect.push_back( Tables::Data_StringData ) ;
      else {
        std::cout << ">> unrecognized schema_datatype '" << this_dt << "'" << std::endl ;
        exit( 1 ) ;
      }
    }
    auto record_data_types = builder.CreateVector( record_data_type_vect ) ;

    // establish rids_vect
    std::vector< uint64_t > rids_vect ;
    rids_vect.push_back( 1 ) ;
    rids_vect.push_back( 2 ) ;
    rids_vect.push_back( 3 ) ;
    for( int i = 0; i < nrows; i++ ) {
      rids_vect.push_back( i+1 ) ;
    }
    auto rids_vect_fb = builder.CreateVector( rids_vect ) ;

    // --------------------------------------------- //
    // read data from file into general structure
    // --------------------------------------------- //
    filedata_t filedata ;
    // initialize struct with empty vects
    for( int i = 0; i < schema_datatypes.size(); i++ ) {
      std::string this_dt = schema_datatypes[i] ;
      if( this_dt == "int" ) {
        std::string key = "att" + std::to_string( i ) + "-int" ;
        // ---- >>>>
        std::vector< std::string > an_index_pair ;
        an_index_pair.push_back( key ) ;
        an_index_pair.push_back( std::to_string( filedata.int_vect_cnt ) ) ;
        filedata.indexer.push_back( an_index_pair ) ;
        // <<<< ----
        std::vector< uint64_t > an_empty_vect ;
        filedata.listof_int_vect_raw.push_back( an_empty_vect ) ;
        filedata.int_vect_cnt++ ;
      }
      else if( this_dt == "float" ) {
        std::string key = "att" + std::to_string( i ) + "-float" ;
        // ---- >>>>
        std::vector< std::string > an_index_pair ;
        an_index_pair.push_back( key ) ;
        an_index_pair.push_back( std::to_string( filedata.float_vect_cnt ) ) ;
        filedata.indexer.push_back( an_index_pair ) ;
        // <<<< ----
        std::vector< float > an_empty_vect ;
        filedata.listof_float_vect_raw.push_back( an_empty_vect ) ;
        filedata.float_vect_cnt++ ;
      }
      else if( this_dt == "string" ) {
        std::string key = "att" + std::to_string( i ) + "-string" ;
        // ---- >>>>
        std::vector< std::string > an_index_pair ;
        an_index_pair.push_back( key ) ;
        an_index_pair.push_back( std::to_string( filedata.string_vect_cnt ) ) ;
        filedata.indexer.push_back( an_index_pair ) ;
        // <<<< ----
        std::vector< std::string > an_empty_vect_strs ;
        std::vector< flatbuffers::Offset<flatbuffers::String> > an_empty_vect ;
        filedata.listof_string_vect_raw_strs.push_back( an_empty_vect_strs ) ;
        filedata.listof_string_vect_raw.push_back( an_empty_vect ) ;
        filedata.string_vect_cnt++ ;
      }
      else {
        std::cout << ">>2 unrecognized schema_datatype '" << this_dt << "'" << std::endl ;
        exit( 1 ) ;
      }
    } //for loop

    // read csv (pipe delimited) from file
    std::ifstream infile( inputs.filename ) ;
    std::string line ;
    int itnum = 0 ; //need this for some reason?
    while ( infile.good() && itnum < nrows ) {
      getline ( infile, line ) ;
      std::vector< std::string > data_vect = parse_psv_str( line ) ;
      for( int i = 0; i < data_vect.size(); i++ ) {
        int attnum = i ;
        std::string atttype = schema_datatypes[i] ;
        std::string attkey  = "att" + std::to_string( attnum ) + "-" + atttype ;
        uint64_t att_vect_id = filedata.get_index( attkey ) ;
        std::string datum = data_vect[i] ;
        std::cout << attkey << "," << att_vect_id <<  "," << datum << ";" ;

        // int
        if( atttype == "int" ) {
          filedata.listof_int_vect_raw[ att_vect_id ].push_back( std::stoi( datum ) ) ;
        }
        // float
        else if( atttype == "float" ) {
          filedata.listof_float_vect_raw[ att_vect_id ].push_back( std::stof( datum ) ) ;
        }
        // string
        else if( atttype == "string" ) {
          filedata.listof_string_vect_raw_strs[ att_vect_id ].push_back( datum ) ;
          filedata.listof_string_vect_raw[ att_vect_id ].push_back( builder.CreateString( datum ) ) ;
        }
        // oops
        else {
          std::cout << ">>3 unrecognized atttype '" << atttype << "'" << std::endl ;
          exit( 1 ) ;
        }
      }
      std::cout << std::endl ;
      itnum++ ;
    }
    infile.close();

    filedata.toString() ;

    // --------------------------------------------- //
    // build out Rows flatbuffer
    // --------------------------------------------- //
    std::vector< flatbuffers::Offset< Tables::Record > > row_records ;

    // create Records as lists of Datas
    for( unsigned int i = 0; i < nrows; i++ ) {

      // record data
      std::vector< flatbuffers::Offset< void > > data_vect ;

      for( unsigned int j = 0; j < filedata.indexer.size(); j++ ) {

        auto key   = filedata.indexer[j][0] ;
        auto index = filedata.indexer[j][1] ;

        if( boost::algorithm::ends_with( key, "-int" ) ) {
          std::vector< uint64_t > single_iv ;
          single_iv.push_back( filedata.listof_int_vect_raw[ std::stoi( index ) ][ i ] ) ;
          auto int_vect_fb = builder.CreateVector( single_iv ) ;
          auto iv = Tables::CreateIntData( builder, int_vect_fb ) ;
          data_vect.push_back( iv.Union() ) ;
        }
        else if( boost::algorithm::ends_with( key, "-float" ) ) {
          std::vector< float > single_fv ;
          single_fv.push_back( filedata.listof_float_vect_raw[ std::stoi( index ) ][ i ] ) ;
          auto float_vect_fb  = builder.CreateVector( single_fv ) ;
          auto fv = Tables::CreateFloatData( builder, float_vect_fb ) ;
          data_vect.push_back( fv.Union() ) ;
        }
        else if( boost::algorithm::ends_with( key, "-string" ) ) {
          std::vector< flatbuffers::Offset<flatbuffers::String> > single_sv ;
          single_sv.push_back( filedata.listof_string_vect_raw[ std::stoi( index ) ][ i ] ) ;
          auto string_vect_fb = builder.CreateVector( single_sv ) ;
          auto sv = Tables::CreateStringData( builder, string_vect_fb ) ;
          data_vect.push_back( sv.Union() ) ;
        }
        else {
          std::cout << ">>> unrecognized key '" << key << "'" << std::endl ;
          exit( 1 ) ;
        }
      } // for loop : creates one data_vect row

      auto data = builder.CreateVector( data_vect ) ;
      auto rec = Tables::CreateRecord( builder, record_data_types, data ) ;
      row_records.push_back( rec ) ;

    } // for loop : processes and saves all rows

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
    //root_builder.add_schema( a ) ;

    // save the Rows flatbuffer to the root flatbuffer
    root_builder.add_relationData_type( Tables::Relation_Rows ) ;
    root_builder.add_relationData( rows.Union() ) ;

    auto res = root_builder.Finish() ;
    builder.Finish( res ) ;

    const char* fb = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;
    int bufsz      = builder.GetSize() ;
    librados::bufferlist bl;
    bl.append( fb, bufsz ) ;
    librados::bufferlist wrapper_bl ;
    ::encode( bl, wrapper_bl ) ;

    if( inputs.writeto == "ceph" ) {
      writeToCeph( wrapper_bl, bufsz, inputs.targetoid, inputs.targetpool ) ;
    }
    else if( inputs.writeto == "disk" ) {
      writeToDisk( wrapper_bl, bufsz, inputs.targetoid ) ;
    } 
    else {
      std::cout << ">>> unrecognized writeto '" << inputs.writeto << "'" << std::endl ;
      exit( 1 ) ;
    }
  }

  // -----------------------------------------------
  // | FBMeta flatbuffer     | Col flatbuffer     |
  // -----------------------------------------------
  else if( inputs.write_type == "col" ) {

    // --------------------------------------------- //
    // --------------------------------------------- //
    uint64_t nrows = inputs.nrows ;
    uint64_t ncols = inputs.ncols ;

    // from commandline
    // place these before record_builder declare
    auto table_name     = builder.CreateString( inputs.table_name ) ;
    auto layout         = builder.CreateString( inputs.write_type ) ;

    // parse schema csv strings
    std::vector< std::string > schema_attnames = parse_csv_str( inputs.schema_attnames ) ;
    std::vector< flatbuffers::Offset< flatbuffers::String > > schema ;
    for( int i = 0; i < schema_attnames.size(); i++ ) {
      schema.push_back( builder.CreateString( schema_attnames[i] ) ) ;
    }
    auto schema_fb = builder.CreateVector( schema ) ;

    std::vector< std::string > schema_datatypes = parse_csv_str( inputs.schema_datatypes ) ;
    std::vector< uint8_t > record_data_type_vect ;
    for( int i = 0; i < schema_datatypes.size(); i++ ) {
      std::string this_dt = schema_datatypes[i] ;
      if( this_dt == "int" )
        record_data_type_vect.push_back( Tables::Data_IntData ) ;
      else if( this_dt == "float" )
        record_data_type_vect.push_back( Tables::Data_FloatData ) ;
      else if( this_dt == "string" )
        record_data_type_vect.push_back( Tables::Data_StringData ) ;
      else {
        std::cout << ">> unrecognized schema_datatype '" << this_dt << "'" << std::endl ;
        exit( 1 ) ;
      }
    }
    auto record_data_types = builder.CreateVector( record_data_type_vect ) ;

    // establish rids_vect
    std::vector< uint64_t > rids_vect ;
    rids_vect.push_back( 1 ) ;
    rids_vect.push_back( 2 ) ;
    rids_vect.push_back( 3 ) ;
    for( int i = 0; i < nrows; i++ ) {
      rids_vect.push_back( i+1 ) ;
    }
    auto rids_vect_fb = builder.CreateVector( rids_vect ) ;

    // --------------------------------------------- //
    // read data from file into general structure
    // --------------------------------------------- //
    filedata_t filedata ;
    // initialize struct with empty vects
    for( int i = 0; i < schema_datatypes.size(); i++ ) {
      std::string this_dt = schema_datatypes[i] ;
      if( this_dt == "int" ) {
        std::string key = "att" + std::to_string( i ) + "-int" ;
        // ---- >>>>
        std::vector< std::string > an_index_pair ;
        an_index_pair.push_back( key ) ;
        an_index_pair.push_back( std::to_string( filedata.int_vect_cnt ) ) ;
        filedata.indexer.push_back( an_index_pair ) ;
        // <<<< ----
        std::vector< uint64_t > an_empty_vect ;
        filedata.listof_int_vect_raw.push_back( an_empty_vect ) ;
        filedata.int_vect_cnt++ ;
      }
      else if( this_dt == "float" ) {
        std::string key = "att" + std::to_string( i ) + "-float" ;
        // ---- >>>>
        std::vector< std::string > an_index_pair ;
        an_index_pair.push_back( key ) ;
        an_index_pair.push_back( std::to_string( filedata.float_vect_cnt ) ) ;
        filedata.indexer.push_back( an_index_pair ) ;
        // <<<< ----
        std::vector< float > an_empty_vect ;
        filedata.listof_float_vect_raw.push_back( an_empty_vect ) ;
        filedata.float_vect_cnt++ ;
      }
      else if( this_dt == "string" ) {
        std::string key = "att" + std::to_string( i ) + "-string" ;
        // ---- >>>>
        std::vector< std::string > an_index_pair ;
        an_index_pair.push_back( key ) ;
        an_index_pair.push_back( std::to_string( filedata.string_vect_cnt ) ) ;
        filedata.indexer.push_back( an_index_pair ) ;
        // <<<< ----
        std::vector< std::string > an_empty_vect_strs ;
        std::vector< flatbuffers::Offset<flatbuffers::String> > an_empty_vect ;
        filedata.listof_string_vect_raw_strs.push_back( an_empty_vect_strs ) ;
        filedata.listof_string_vect_raw.push_back( an_empty_vect ) ;
        filedata.string_vect_cnt++ ;
      }
      else {
        std::cout << ">>2 unrecognized schema_datatype '" << this_dt << "'" << std::endl ;
        exit( 1 ) ;
      }
    } //for loop

    // read csv (pipe delimited) from file
    std::ifstream infile( inputs.filename ) ;
    std::string line ;
    int itnum = 0 ; //need this for some reason?
    while ( infile.good() && itnum < nrows ) {
      getline ( infile, line ) ;
      std::vector< std::string > data_vect = parse_psv_str( line ) ;
      for( int i = 0; i < data_vect.size(); i++ ) {
        int attnum = i ;
        std::string atttype = schema_datatypes[i] ;
        std::string attkey  = "att" + std::to_string( attnum ) + "-" + atttype ;
        uint64_t att_vect_id = filedata.get_index( attkey ) ;
        std::string datum = data_vect[i] ;
        std::cout << attkey << "," << att_vect_id <<  "," << datum << ";" ;

        // int
        if( atttype == "int" ) {
          filedata.listof_int_vect_raw[ att_vect_id ].push_back( std::stoi( datum ) ) ;
        }
        // float
        else if( atttype == "float" ) {
          filedata.listof_float_vect_raw[ att_vect_id ].push_back( std::stof( datum ) ) ;
        }
        // string
        else if( atttype == "string" ) {
          filedata.listof_string_vect_raw_strs[ att_vect_id ].push_back( datum ) ;
          filedata.listof_string_vect_raw[ att_vect_id ].push_back( builder.CreateString( datum ) ) ;
        }
        // oops
        else {
          std::cout << ">>3 unrecognized atttype '" << atttype << "'" << std::endl ;
          exit( 1 ) ;
        }
      }
      std::cout << std::endl ;
      itnum++ ;
    }
    infile.close();

    filedata.toString() ;

    // --------------------------------------------- //
    // build out Col flatbuffers
    // --------------------------------------------- //

    // bufferlist collecting all Col flatbuffer bufferlists
    librados::bufferlist bl_seq ;
    int buffer_size = 0 ;

    for( unsigned int i = 0; i < ncols; i++ ) {
      auto col_name     = schema[ i ] ;
      uint8_t col_index = (uint8_t)i ;
      uint8_t nrows_fb  = (uint8_t)nrows ;
      auto RIDs         = builder.CreateVector( rids_vect ) ;

      auto key   = filedata.indexer[i][0] ;
      auto index = filedata.indexer[i][1] ;

      if( boost::algorithm::ends_with( key, "-int" ) ) {
        std::vector< uint64_t > int_vect = filedata.listof_int_vect_raw[ std::stoi(index) ] ;
        auto int_vect_fb = builder.CreateVector( int_vect ) ;
        auto data = Tables::CreateIntData( builder, int_vect_fb ) ;
        auto col = Tables::CreateCol(
          builder,
          0,
          0,
          col_name,
          col_index,
          nrows_fb,
          RIDs,
          Tables::Data_IntData,
          data.Union() ) ;

        Tables::RootBuilder root_builder( builder ) ;
        root_builder.add_relationData_type( Tables::Relation_Col ) ;
        root_builder.add_relationData( col.Union() ) ;
        auto res = root_builder.Finish() ;
        builder.Finish( res ) ;

        // save each Col flatbuffer in one bufferlist and
        // append the bufferlist to a larger bufferlist.
        const char* fb = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;
        int bufsz = builder.GetSize() ;
        librados::bufferlist bl ;
        bl.append( fb, bufsz ) ;
        ::encode( bl, bl_seq ) ;
        buffer_size = buffer_size + bufsz ;
      }
      else if( boost::algorithm::ends_with( key, "-float" ) ) {
        std::vector< float > float_vect = filedata.listof_float_vect_raw[ std::stoi(index) ] ;
        auto float_vect_fb = builder.CreateVector( float_vect ) ;
        auto data = Tables::CreateFloatData( builder, float_vect_fb ) ;
        auto col = Tables::CreateCol(
          builder,
          0,
          0,
          col_name,
          col_index,
          nrows_fb,
          RIDs,
          Tables::Data_FloatData,
          data.Union() ) ;

        Tables::RootBuilder root_builder( builder ) ;
        root_builder.add_relationData_type( Tables::Relation_Col ) ;
        root_builder.add_relationData( col.Union() ) ;
        auto res = root_builder.Finish() ;
        builder.Finish( res ) ;

        // save each Col flatbuffer in one bufferlist and
        // append the bufferlist to a larger bufferlist.
        const char* fb = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;
        int bufsz = builder.GetSize() ;
        librados::bufferlist bl ;
        bl.append( fb, bufsz ) ;
        ::encode( bl, bl_seq ) ;
        buffer_size = buffer_size + bufsz ;
      }
      else if( boost::algorithm::ends_with( key, "-string" ) ) {
        std::vector< flatbuffers::Offset<flatbuffers::String> > string_vect = filedata.listof_string_vect_raw[ std::stoi(index) ] ;
        auto string_vect_fb = builder.CreateVector( string_vect ) ;
        auto data = Tables::CreateStringData( builder, string_vect_fb ) ;
        auto col = Tables::CreateCol(
          builder,
          0,
          0,
          col_name,
          col_index,
          nrows_fb,
          RIDs,
          Tables::Data_StringData,
          data.Union() ) ;

        Tables::RootBuilder root_builder( builder ) ;
        root_builder.add_relationData_type( Tables::Relation_Col ) ;
        root_builder.add_relationData( col.Union() ) ;
        auto res = root_builder.Finish() ;
        builder.Finish( res ) ;

        // save each Col flatbuffer in one bufferlist and
        // append the bufferlist to a larger bufferlist.
        const char* fb = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;
        int bufsz = builder.GetSize() ;
        librados::bufferlist bl ;
        bl.append( fb, bufsz ) ;
        ::encode( bl, bl_seq ) ;
        buffer_size = buffer_size + bufsz ;
      }
    }

    // write bufferlist
    if( inputs.writeto == "ceph" ) {
      writeToCeph( bl_seq, buffer_size, inputs.targetoid, inputs.targetpool ) ;
    }
    else if( inputs.writeto == "disk" ) {
      writeToDisk( bl_seq, buffer_size, inputs.targetoid ) ;
    } 
    else {
      std::cout << ">>> unrecognized writeto '" << inputs.writeto << "'" << std::endl ;
      exit( 1 ) ;
    }
  }

  // otherwise oops
  else {
    std::cout << ">> unrecognized inputs.write_type = '" << inputs.write_type << "'" << std::endl ;
  }
} // do_write
