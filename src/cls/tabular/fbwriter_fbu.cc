/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


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
#include "fbu_generated.h"

#include "cls_tabular_utils.h"

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
    for( unsigned int i = 0; i < listof_int_vect_raw.size(); i++ ) {
      std::cout << "listof_int_vect_raw[" << std::to_string( i ) << "].size() = " 
                << listof_int_vect_raw[i].size() << std::endl ;
      for( unsigned int j = 0; j < listof_int_vect_raw[i].size(); j++ ) {
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
    for( unsigned int i = 0; i < listof_float_vect_raw.size(); i++ ) {
      std::cout << "listof_float_vect_raw[" << std::to_string( i ) << "].size() = " 
                << listof_float_vect_raw[i].size() << std::endl ;
      for( unsigned int j = 0; j < listof_float_vect_raw[i].size(); j++ ) {
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
    for( unsigned int i = 0; i < listof_string_vect_raw.size(); i++ ) {
      std::cout << "listof_string_vect_raw[" << std::to_string( i ) << "].size() = " 
                << listof_string_vect_raw[i].size() << std::endl ;
      for( unsigned int j = 0; j < listof_string_vect_raw[i].size(); j++ ) {
        std::cout << listof_string_vect_raw_strs[i][j] ;
        if( j < listof_string_vect_raw[i].size()-1 )
          std::cout << "," ;
      }
      std::cout << std::endl ;
    }
  } // toString

  uint64_t get_index( std::string in_key ) {
    for( unsigned int i = 0; i < indexer.size(); i++ ) {
      std::string key   = indexer[i][0] ;
      std::string index = indexer[i][1] ;
      if( in_key == key) {
        return std::stoi( index ) ;
      }
    }
    return -1 ;
  } // get_index
} ;

struct cmdline_inputs_t {
  bool debug ;
  std::string write_type ;
  std::string filename ;
  std::string schema_datatypes ;
  std::string schema_attnames ;
  std::string table_name ;
  uint64_t nrows ;
  uint64_t ncols ;
  uint64_t cols_per_fb ;
  std::string writeto ;
  std::string targetformat ;
  std::string targetoid ;
  std::string targetpool ;
} ;

int writeToDisk( librados::bufferlist wrapper_bl, 
                 int bufsz, 
                 std::string target_format, 
                 std::string target_oid ) {
  int mode = 0600 ;
  std::string fname = "skyhook."+ target_format + "." + target_oid + ".0" ;
  wrapper_bl.write_file( fname.c_str(), mode ) ;
  printf( "buff size: %d, wrapper_bl size: %d\n", bufsz, wrapper_bl.length() ) ;

  return 0;
}

int writeToCeph( librados::bufferlist bl_seq, 
                 int bufsz, 
                 std::string target_format, 
                 std::string target_oid, 
                 std::string target_pool ) {

  // save to ceph object
  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  checkret( ret, 0 ) ;

  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( target_pool.c_str(), ioctx ) ;

  // write bl_seq to ceph object
  const char *obj_name = target_oid.c_str() ;
  bufferlist::iterator p = bl_seq.begin();
  size_t i = p.get_remaining() ;
  std::cout << "num bytes written : " << i << std::endl ;
  ret = ioctx.write( obj_name, bl_seq, i, 0 ) ;

  ioctx.close() ;
  return 0 ;
}

void do_write( cmdline_inputs_t, bool ) ;
std::string getSchemaString( uint64_t ncols,
                             std::vector< std::string > schema_attnames,
                             std::vector< Tables::SkyDataType > schema_datatypes_sdt,
                             bool debug ) ;

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
  std::string write_type ;
  std::string filename ;
  std::string schema_datatypes ;
  std::string schema_attnames ;
  std::string table_name ;
  uint64_t nrows ;
  uint64_t ncols ;
  uint64_t cols_per_fb ;
  std::string writeto ;
  std::string targetformat ;
  std::string targetoid ;
  std::string targetpool ;

  po::options_description gen_opts("General options");
  gen_opts.add_options()
    ("help,h", "show help message")
    ("debug", po::value<bool>(&debug)->required(), "debug")
    ("write_type", po::value<std::string>(&write_type)->required(), "write_type")
    ("filename", po::value<std::string>(&filename)->required(), "filename")
    ("schema_datatypes", po::value<std::string>(&schema_datatypes)->required(), "schema_datatypes")
    ("schema_attnames", po::value<std::string>(&schema_attnames)->required(), "schema_attnames")
    ("table_name", po::value<std::string>(&table_name)->required(), "table_name")
    ("nrows", po::value<uint64_t>(&nrows)->required(), "nrows")
    ("ncols", po::value<uint64_t>(&ncols)->required(), "ncols")
    ("cols_per_fb", po::value<uint64_t>(&cols_per_fb), "cols_per_fb")
    ("writeto", po::value<std::string>(&writeto)->required(), "writeto")
    ("targetformat", po::value<std::string>(&targetformat)->required(), "targetformat")
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
  inputs.debug            = debug ;
  inputs.write_type       = write_type ;
  inputs.filename         = filename ;
  inputs.schema_datatypes = schema_datatypes ;
  inputs.schema_attnames  = schema_attnames ;
  inputs.table_name       = table_name ;
  inputs.nrows            = nrows ;
  inputs.ncols            = ncols ;
  inputs.writeto          = writeto ;
  inputs.targetformat     = targetformat ;
  inputs.targetoid        = targetoid ;
  inputs.targetpool       = targetpool ;
  inputs.cols_per_fb      = cols_per_fb ;

  do_write( inputs, debug ) ;

  return 0 ;
} // main

// =========== //
//   DO WRITE  //
// =========== //
void do_write( cmdline_inputs_t inputs, bool debug ) {

  if( inputs.debug ) {
    std::cout << "inputs.debug            : " << inputs.debug                   << std::endl ;
    std::cout << "inputs.write_type       : " << inputs.write_type              << std::endl ;
    std::cout << "inputs.filename         : " << inputs.filename                << std::endl ;
    std::cout << "inputs.schema_datatypes : " << inputs.schema_datatypes        << std::endl ;
    std::cout << "inputs.schema_attnames  : " << inputs.schema_attnames         << std::endl ;
    std::cout << "inputs.table_name       : " << inputs.table_name              << std::endl ;
    std::cout << "inputs.nrows            : " << std::to_string( inputs.nrows ) << std::endl ;
    std::cout << "inputs.ncols            : " << std::to_string( inputs.ncols ) << std::endl ;
    std::cout << "inputs.writeto          : " << inputs.writeto << std::endl ;
    std::cout << "inputs.targetformat     : " << inputs.targetformat << std::endl ;
    std::cout << "inputs.targetoid        : " << inputs.targetoid << std::endl ;
    std::cout << "inputs.targetpool       : " << inputs.targetpool << std::endl ;
    std::cout << "inputs.cols_per_fb      : " << std::to_string( inputs.cols_per_fb ) << std::endl ;
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
    uint64_t nrows  = inputs.nrows ;
    uint64_t ncols  = inputs.ncols ;
    auto table_name = builder.CreateString( inputs.table_name ) ;

    // parse schema csv strings
    std::vector< std::string > schema_attnames = parse_csv_str( inputs.schema_attnames ) ;
    std::vector< flatbuffers::Offset< flatbuffers::String > > schema ;
    for( unsigned int i = 0; i < schema_attnames.size(); i++ )
      schema.push_back( builder.CreateString( schema_attnames[i] ) ) ;

    std::vector< std::string > schema_datatypes = parse_csv_str( inputs.schema_datatypes ) ;
    std::vector< Tables::SkyDataType > schema_datatypes_sdt ;
    std::vector< uint8_t > record_data_type_vect ;
    for( unsigned int i = 0; i < schema_datatypes.size(); i++ ) {
      auto this_dt = schema_datatypes[i] ;
      if( this_dt == "int" ) {
        record_data_type_vect.push_back( Tables::DataTypes_FBU_SDT_UINT64_FBU ) ;
        schema_datatypes_sdt.push_back( Tables::SDT_UINT64 ) ;
      }
      else if( this_dt == "float" ) {
        record_data_type_vect.push_back( Tables::DataTypes_FBU_SDT_FLOAT_FBU ) ;
        schema_datatypes_sdt.push_back( Tables::SDT_FLOAT ) ;
      }
      else if( this_dt == "string" ) {
        record_data_type_vect.push_back( Tables::DataTypes_FBU_SDT_STRING_FBU ) ;
        schema_datatypes_sdt.push_back( Tables::SDT_STRING ) ;
      }
      else {
        std::cout << ">> unrecognized schema_datatype '" << this_dt << "'" << std::endl ;
        exit( 1 ) ;
      }
    }//for loop
    auto record_data_types = builder.CreateVector( record_data_type_vect ) ;

    // --------------------------------------------- //
    // read data from file into general structure
    // --------------------------------------------- //
    filedata_t filedata ;
    // initialize struct with empty vects
    for( unsigned int i = 0; i < schema_datatypes_sdt.size(); i++ ) {
      Tables::SkyDataType this_dt = schema_datatypes_sdt[i] ;
      switch( this_dt ) {
        case Tables::SDT_UINT64 : {
          std::string key = "att" + std::to_string( i ) + "-" + std::to_string( this_dt ) ;
          // ---- >>>>
          std::vector< std::string > an_index_pair ;
          an_index_pair.push_back( key ) ;
          an_index_pair.push_back( std::to_string( filedata.int_vect_cnt ) ) ;
          filedata.indexer.push_back( an_index_pair ) ;
          // <<<< ----
          std::vector< uint64_t > an_empty_vect ;
          filedata.listof_int_vect_raw.push_back( an_empty_vect ) ;
          filedata.int_vect_cnt++ ;
          break ;
        }
        case Tables::SDT_FLOAT : {
          std::string key = "att" + std::to_string( i ) + "-" + std::to_string( this_dt ) ;
          // ---- >>>>
          std::vector< std::string > an_index_pair ;
          an_index_pair.push_back( key ) ;
          an_index_pair.push_back( std::to_string( filedata.float_vect_cnt ) ) ;
          filedata.indexer.push_back( an_index_pair ) ;
          // <<<< ----
          std::vector< float > an_empty_vect ;
          filedata.listof_float_vect_raw.push_back( an_empty_vect ) ;
          filedata.float_vect_cnt++ ;
          break ;
        }
        case Tables::SDT_STRING : {
          std::string key = "att" + std::to_string( i ) + "-" + std::to_string( this_dt ) ;
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
          break ;
        }
        default :
          std::cout << ">>2 unrecognized schema_datatype '" << this_dt << "'" << std::endl ;
          exit( 1 ) ;
      }//switch
    } //for loop

    // read csv (pipe delimited) from file
    std::ifstream infile( inputs.filename ) ;
    std::string line ;
    int itnum = 0 ; //need this for some reason?
    while ( infile.good() && (unsigned)itnum < nrows ) {
      getline ( infile, line ) ;
      std::vector< std::string > data_vect = parse_psv_str( line ) ;
      for( unsigned int i = 0; i < data_vect.size(); i++ ) {
        int attnum = i ;
        Tables::SkyDataType atttype = schema_datatypes_sdt[i] ;
        std::string attkey          = "att" + std::to_string( attnum ) + "-" + std::to_string( atttype ) ;
        uint64_t att_vect_id        = filedata.get_index( attkey ) ;
        std::string datum           = data_vect[i] ;

        if( debug )
          std::cout << attkey << "," << att_vect_id <<  "," << datum << ";" ;

        switch( atttype ) {
          case Tables::SDT_UINT64 :
            filedata.listof_int_vect_raw[ att_vect_id ].push_back( std::stoi( datum ) ) ;
            break ;
          case Tables::SDT_FLOAT :
            filedata.listof_float_vect_raw[ att_vect_id ].push_back( std::stof( datum ) ) ;
            break ;
          case Tables::SDT_STRING :
            filedata.listof_string_vect_raw_strs[ att_vect_id ].push_back( datum ) ;
            filedata.listof_string_vect_raw[ att_vect_id ].push_back( builder.CreateString( datum ) ) ;
            break ;
          default :
            std::cout << "\n>>3 unrecognized atttype '" << std::to_string( atttype ) << "'" << std::endl ;
            exit( 1 ) ;
        } //switch

      } //for
      if( debug )
        std::cout << std::endl ;
      itnum++ ;
    } //while
    infile.close() ;

    if( debug )
      filedata.toString() ;

    // --------------------------------------------- //
    // build out Rows flatbuffer
    // --------------------------------------------- //
    std::vector< flatbuffers::Offset< Tables::Record_FBU > > row_records ;
    std::vector< uint64_t > nullbits_vector ( 2, 0 ) ; //initialize with one 0 per row.
    auto nullbits_vector_fb = builder.CreateVector( nullbits_vector ) ;

    // create Records as lists of Datas
    for( unsigned int i = 0; i < nrows; i++ ) {

      // record data
      std::vector< flatbuffers::Offset< void > > data_vect ;

      for( unsigned int j = 0; j < filedata.indexer.size(); j++ ) {

        auto key   = filedata.indexer[j][0] ;
        auto index = filedata.indexer[j][1] ;

        if( boost::algorithm::ends_with( key, std::to_string( Tables::SDT_UINT64 ) ) ) {
          std::vector< uint64_t > single_iv ;
          single_iv.push_back( filedata.listof_int_vect_raw[ std::stoi( index ) ][ i ] ) ;
          auto int_vect_fb = builder.CreateVector( single_iv ) ;
          auto iv = Tables::CreateSDT_UINT64_FBU( builder, int_vect_fb ) ;
          data_vect.push_back( iv.Union() ) ;
        }
        else if( boost::algorithm::ends_with( key, std::to_string( Tables::SDT_FLOAT ) ) ) {
          std::vector< float > single_fv ;
          single_fv.push_back( filedata.listof_float_vect_raw[ std::stoi( index ) ][ i ] ) ;
          auto float_vect_fb  = builder.CreateVector( single_fv ) ;
          auto fv = Tables::CreateSDT_FLOAT_FBU( builder, float_vect_fb ) ;
          data_vect.push_back( fv.Union() ) ;
        }
        else if( boost::algorithm::ends_with( key, std::to_string( Tables::SDT_STRING ) ) ) {
          std::vector< flatbuffers::Offset<flatbuffers::String> > single_sv ;
          single_sv.push_back( filedata.listof_string_vect_raw[ std::stoi( index ) ][ i ] ) ;
          auto string_vect_fb = builder.CreateVector( single_sv ) ;
          auto sv = Tables::CreateSDT_STRING_FBU( builder, string_vect_fb ) ;
          data_vect.push_back( sv.Union() ) ;
        }
        else {
          std::cout << ">>> unrecognized key '" << key << "'" << std::endl ;
          exit( 1 ) ;
        }
      } // for loop : creates one data_vect row

      auto data = builder.CreateVector( data_vect ) ;
      auto rec = Tables::CreateRecord_FBU(
        builder,                 //builder ptr
        i,                       //rid
        nullbits_vector_fb,      //nullbits vect
        record_data_types,       //data types vect
        data ) ;                 //data vect
      row_records.push_back( rec ) ;

    } // for loop : processes and saves all rows

    auto row_records_fb = builder.CreateVector( row_records ) ;

    // create the Rows flatbuffer:
    auto rows = Tables::CreateRows_FBU(
      builder,            //builder
      row_records_fb ) ;  //data

    // generate schema string
    std::string schema_string = "" ;
    for( unsigned int i = 0; i < ncols; i++ ) {
      std::string att_name = schema_attnames[ i ] ;
      auto att_type = schema_datatypes_sdt[ i ] ;
      std::string this_entry = " " + std::to_string( i ) + " " ;
      switch( att_type ) {
        case Tables::SDT_UINT64 :
          this_entry = this_entry + std::to_string( Tables::SDT_UINT64 ) ;
          break ;
        case Tables::SDT_FLOAT :
          this_entry = this_entry + std::to_string( Tables::SDT_FLOAT ) ;
          break ;
        case Tables::SDT_STRING :
          this_entry = this_entry + std::to_string( Tables::SDT_STRING ) ;
          break ;
        default :
          std::cout << ">>4 unrecognized att_type '" << att_type << "'" << std::endl ;
          exit( 1 ) ;
      } //switch
      this_entry = this_entry + " 0 0 " + att_name + " \n" ;
      schema_string = schema_string + this_entry ;
    } //for

    auto schema_string_fb = builder.CreateString( schema_string ) ;
    if( debug )
      std::cout << "schema_string = " << schema_string << std::endl ;

    auto db_schema_name = builder.CreateString( "kats_test" ) ;
    std::vector< uint8_t > delete_vector ( nrows, 0 ) ; //initialize with one 0 per row.
    auto delete_vector_fb = builder.CreateVector( delete_vector ) ;

    auto root = CreateRoot_FBU(
      builder,                          //builder
      SFT_FLATBUF_UNION_ROW,           //data_format_type
      0,                                //skyhook_version
      0,                                //data_structure_version
      0,                                //data_schema_version
      schema_string_fb,                 //data_schema string
      db_schema_name,                   //db_schema_name TODO: parameterize
      (uint32_t)nrows,                  //nrows
      (uint32_t)ncols,                  //ncols
      table_name,                       //table_name
      delete_vector_fb,                 //delete_vector
      Tables::Relation_FBU_Rows_FBU,    //relationData_type
      rows.Union() ) ;                  //relationData

    builder.Finish( root ) ;

    //const char* dataptr = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;
    char* dataptr = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;
    int datasz          = builder.GetSize() ;
    librados::bufferlist bl;
    bl.append( dataptr, datasz ) ;
    librados::bufferlist wrapper_bl ;
    ::encode( bl, wrapper_bl ) ;

    std::cout << "datasz = " << datasz << std::endl ;
    std::cout << "wrapper_bl.length() = " << wrapper_bl.length() << std::endl ;

    // --------------------------------------------- //
    // build out FB_Meta
    // --------------------------------------------- //
    flatbuffers::FlatBufferBuilder *meta_builder = new flatbuffers::FlatBufferBuilder();
    Tables::createFbMeta( meta_builder, 
                          SFT_FLATBUF_UNION_ROW,
                          reinterpret_cast<unsigned char*>( wrapper_bl.c_str() ),
                          wrapper_bl.length() ) ;

    // add meta_builder's data into a bufferlist as char*
    ceph::bufferlist meta_bl ;
    char* meta_builder_ptr = reinterpret_cast<char*>( meta_builder->GetBufferPointer() ) ;
    int meta_builder_size  = meta_builder->GetSize() ;
    std::cout << "meta_builder_size = " << meta_builder_size << std::endl ;
    meta_bl.append( meta_builder_ptr, meta_builder_size ) ;
    delete meta_builder;
    librados::bufferlist meta_wrapper_bl ;
    ::encode( meta_bl, meta_wrapper_bl ) ;
    size_t meta_wrapper_bl_sz = meta_wrapper_bl.length() ;
    std::cout << "meta_wrapper_bl_sz = " << meta_wrapper_bl_sz << std::endl ;

    // --------------------------------------------- //
    // do the write
    // --------------------------------------------- //
    if( inputs.writeto == "ceph" )
      //writeToCeph( wrapper_bl, datasz, inputs.targetformat, inputs.targetoid, inputs.targetpool ) ;
      writeToCeph( meta_wrapper_bl, meta_builder_size, inputs.targetformat, inputs.targetoid, inputs.targetpool ) ;
    else if( inputs.writeto == "disk" )
      writeToDisk( wrapper_bl, datasz, inputs.targetformat, inputs.targetoid ) ;
    else {
      std::cout << ">>> unrecognized writeto '" << inputs.writeto << "'" << std::endl ;
      exit( 1 ) ;
    }
  } //write_type==rows

  // -----------------------------------------------
  // | FBMeta flatbuffer     | Cols flatbuffer     |
  // -----------------------------------------------
  else if( inputs.write_type == "cols" ) {

    // make sure cols_per_fb is defined greater than 0
    if( inputs.cols_per_fb <= 0 ) {
      std::cout << "ERROR : invalid cols_per_fb '" << std::to_string( inputs.cols_per_fb ) << "'" << std::endl ;
      std::cout << "        cols_per_fb must be greater 0 for column writes." << std::endl ;
      exit(1) ;
    }

    // --------------------------------------------- //
    // --------------------------------------------- //
    uint64_t nrows      = inputs.nrows ;
    uint64_t ncols      = inputs.ncols ;
    auto table_name     = builder.CreateString( inputs.table_name ) ;
    auto db_schema_name = builder.CreateString( "kats_test" ) ;
    std::vector< uint64_t > nullbits_vector ( 2, 0 ) ; //initialize with one 0 per row.
    auto nullbits_vector_fb = builder.CreateVector( nullbits_vector ) ;

    // parse schema csv strings
    std::vector< std::string > schema_attnames = parse_csv_str( inputs.schema_attnames ) ;
    std::vector< flatbuffers::Offset< flatbuffers::String > > schema ;
    for( unsigned int i = 0; i < schema_attnames.size(); i++ )
      schema.push_back( builder.CreateString( schema_attnames[i] ) ) ;

    std::vector< std::string > schema_datatypes = parse_csv_str( inputs.schema_datatypes ) ;
    std::vector< Tables::SkyDataType > schema_datatypes_sdt ;
    std::vector< uint8_t > record_data_type_vect ;
    for( unsigned int i = 0; i < schema_datatypes.size(); i++ ) {
      std::string this_dt = schema_datatypes[i] ;
      if( this_dt == "int" ) {
        record_data_type_vect.push_back( Tables::DataTypes_FBU_SDT_UINT64_FBU ) ;
        schema_datatypes_sdt.push_back( Tables::SDT_UINT64 ) ;
      }
      else if( this_dt == "float" ) {
        record_data_type_vect.push_back( Tables::DataTypes_FBU_SDT_FLOAT_FBU ) ;
        schema_datatypes_sdt.push_back( Tables::SDT_FLOAT ) ;
      }
      else if( this_dt == "string" ) {
        record_data_type_vect.push_back( Tables::DataTypes_FBU_SDT_STRING_FBU ) ;
        schema_datatypes_sdt.push_back( Tables::SDT_STRING ) ;
      }
      else {
        std::cout << ">> unrecognized schema_datatype '" << this_dt << "'" << std::endl ;
        exit( 1 ) ;
      }
    }

    auto schema_string = getSchemaString( ncols, 
                                          schema_attnames, 
                                          schema_datatypes_sdt,
                                          debug ) ;
    auto schema_string_fb = builder.CreateString( schema_string ) ;

    // establish rids_vect
    std::vector< uint64_t > rids_vect ;
    for( unsigned int i = 0; i < nrows; i++ )
      rids_vect.push_back( i+1 ) ;
    auto rids_vect_fb = builder.CreateVector( rids_vect ) ;

    // --------------------------------------------- //
    // read data from file into general structure
    // --------------------------------------------- //
    filedata_t filedata ;
    // initialize struct with empty vects
    for( unsigned int i = 0; i < schema_datatypes_sdt.size(); i++ ) {
      Tables::SkyDataType this_dt = schema_datatypes_sdt[i] ;
      std::string key = "att" + std::to_string( i ) + "-" + std::to_string( this_dt ) ;
      std::vector< std::string > an_index_pair ;
      an_index_pair.push_back( key ) ;
      switch( this_dt ) {
        case Tables::SDT_UINT64 : {
          an_index_pair.push_back( std::to_string( filedata.int_vect_cnt ) ) ;
          filedata.indexer.push_back( an_index_pair ) ;
          std::vector< uint64_t > an_empty_vect ;
          filedata.listof_int_vect_raw.push_back( an_empty_vect ) ;
          filedata.int_vect_cnt++ ;
          break ;
        }
        case Tables::SDT_FLOAT : {
          an_index_pair.push_back( std::to_string( filedata.float_vect_cnt ) ) ;
          filedata.indexer.push_back( an_index_pair ) ;
          std::vector< float > an_empty_vect ;
          filedata.listof_float_vect_raw.push_back( an_empty_vect ) ;
          filedata.float_vect_cnt++ ;
          break ;
        }
        case Tables::SDT_STRING : {
          an_index_pair.push_back( std::to_string( filedata.string_vect_cnt ) ) ;
          filedata.indexer.push_back( an_index_pair ) ;
          std::vector< std::string > an_empty_vect_strs ;
          std::vector< flatbuffers::Offset<flatbuffers::String> > an_empty_vect ;
          filedata.listof_string_vect_raw_strs.push_back( an_empty_vect_strs ) ;
          filedata.listof_string_vect_raw.push_back( an_empty_vect ) ;
          filedata.string_vect_cnt++ ;
          break ;
        }
        default :
          std::cout << ">>2 unrecognized schema_datatype '" << this_dt << "'" << std::endl ;
          exit( 1 ) ;
      }
    } //for loop

    // read csv (pipe delimited) from file
    std::ifstream infile( inputs.filename ) ;
    std::string line ;
    int itnum = 0 ; //need this for some reason?
    while ( infile.good() && (unsigned)itnum < nrows ) {
      getline ( infile, line ) ;
      std::vector< std::string > data_vect = parse_psv_str( line ) ;
      for( unsigned int i = 0; i < data_vect.size(); i++ ) {
        int attnum = i ;
        auto atttype = schema_datatypes_sdt[i] ;
        std::string attkey  = "att" + std::to_string( attnum ) + "-" + std::to_string( atttype ) ;
        uint64_t att_vect_id = filedata.get_index( attkey ) ;
        std::string datum = data_vect[i] ;

        if( debug )
          std::cout << attkey << "," << att_vect_id <<  "," << datum << ";" ;

        switch( atttype ) {
          case Tables::SDT_UINT64 :
            filedata.listof_int_vect_raw[ att_vect_id ].push_back( std::stoi( datum ) ) ;
            break ;
          case Tables::SDT_FLOAT :
            filedata.listof_float_vect_raw[ att_vect_id ].push_back( std::stof( datum ) ) ;
            break ;
          case Tables::SDT_STRING :
            filedata.listof_string_vect_raw_strs[ att_vect_id ].push_back( datum ) ;
            filedata.listof_string_vect_raw[ att_vect_id ].push_back( builder.CreateString( datum ) ) ;
            break ;
          default :
            std::cout << ">>3 unrecognized atttype '" << atttype << "'" << std::endl ;
            exit( 1 ) ;
        } //switch

      } //for loop

      if( debug )
        std::cout << std::endl ;

      itnum++ ;
    } //while loop
    infile.close();

    if( debug )
      filedata.toString() ;

    // --------------------------------------------- //
    // build out Col flatbuffers
    // --------------------------------------------- //

    // bufferlist collecting all Col flatbuffer bufferlists
    librados::bufferlist bl_seq ;
    int buffer_size = 0 ;

    // Cols_FBU is an array of Col_FBUs.
    // flush to write whenever cols_per_fb divides i.
    std::vector< flatbuffers::Offset< Tables::Col_FBU > > cols_vect ;
    for( unsigned int i = 0; i < ncols; i++ ) {
      auto col_name     = schema[ i ] ;
      uint8_t col_index = (uint8_t)i ;

      auto key   = filedata.indexer[i][0] ;
      auto index = filedata.indexer[i][1] ;

      if( debug )
        std::cout << "processing col " << std::to_string( i ) << std::endl ;

      if( boost::algorithm::ends_with( key, std::to_string( Tables::SDT_UINT64 ) ) ) {
        std::vector< uint64_t > int_vect = filedata.listof_int_vect_raw[ std::stoi(index) ] ;
        auto int_vect_fb = builder.CreateVector( int_vect ) ;
        auto data = Tables::CreateSDT_UINT64_FBU( builder, int_vect_fb ) ;
        auto col = Tables::CreateCol_FBU(
          builder,                              //builder
          rids_vect_fb,                         //rids
          nullbits_vector_fb,                   //nullbits
          col_name,                             //col_name
          col_index,                            //col_index
          Tables::DataTypes_FBU_SDT_UINT64_FBU, //data_type
          data.Union() ) ;                      //data
        cols_vect.push_back( col ) ;
      }
      else if( boost::algorithm::ends_with( key, std::to_string( Tables::SDT_FLOAT ) ) ) {
        std::vector< float > float_vect = filedata.listof_float_vect_raw[ std::stoi(index) ] ;
        auto float_vect_fb = builder.CreateVector( float_vect ) ;
        auto data = Tables::CreateSDT_FLOAT_FBU( builder, float_vect_fb ) ;
        auto col = Tables::CreateCol_FBU(
          builder,                              //builder
          rids_vect_fb,                         //rids
          nullbits_vector_fb,                   //nullbits
          col_name,                             //col_name
          col_index,                            //col_index
          Tables::DataTypes_FBU_SDT_FLOAT_FBU,  //data_type
          data.Union() ) ;                      //data
        cols_vect.push_back( col ) ;
      }
      else if( boost::algorithm::ends_with( key, std::to_string( Tables::SDT_STRING ) ) ) {
        std::vector< flatbuffers::Offset<flatbuffers::String> > string_vect = filedata.listof_string_vect_raw[ std::stoi(index) ] ;
        auto string_vect_fb = builder.CreateVector( string_vect ) ;
        auto data = Tables::CreateSDT_STRING_FBU( builder, string_vect_fb ) ;
        auto col = Tables::CreateCol_FBU(
          builder,                              //builder
          rids_vect_fb,                         //rids
          nullbits_vector_fb,                   //nullbits
          col_name,                             //col_name
          col_index,                            //col_index
          Tables::DataTypes_FBU_SDT_STRING_FBU, //data_type
          data.Union() ) ;                      //data
        cols_vect.push_back( col ) ;
      }
      else {
        std::cout << ">>> unrecognized key '" << key << "'" << std::endl ;
        exit(1) ;
      }

      //save the Cols flatbuffer
      if( inputs.cols_per_fb == 1 ||
          ( (i+1) % inputs.cols_per_fb == 0 ) ||
          ( (i+1) == ncols ) ) {

        if( debug )
          std::cout << "saving bl to bl_seq" << std::endl ;

        auto cols_data = builder.CreateVector( cols_vect ) ;
        std::vector< flatbuffers::Offset< Tables::Col_FBU > > empty_cols_vect ;
        cols_vect = empty_cols_vect ; //empty out collection vector

        auto cols = CreateCols_FBU(
          builder,
          cols_data ) ;

        std::vector< uint8_t > delete_vector ( nrows, 0 ) ; //initialize with one 0 per row.
        auto delete_vector_fb = builder.CreateVector( delete_vector ) ;

        auto root = CreateRoot_FBU(
          builder,                          // builder
          SFT_FLATBUF_UNION_COL,           // data_format_type
          0,                                // skyhook_version
          0,                                // data_structure_version
          0,                                // data_schema_version
          schema_string_fb,                 // data_schema string
          db_schema_name,                   // db_schema_name TODO: parameterize
          (uint32_t)nrows,                  // nrows
          (uint32_t)ncols,                  // ncols
          table_name,                       // table_name
          delete_vector_fb,                 // delete_vector
          Tables::Relation_FBU_Cols_FBU,    // relationData_type
          cols.Union() ) ;                  // relationData

        builder.Finish( root ) ;

        // save each Col flatbuffer in one bufferlist and
        // append the bufferlist to a larger bufferlist.
        const char* dataptr = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;
        int datasz = builder.GetSize() ;
        librados::bufferlist bl ;
        bl.append( dataptr, datasz ) ;
        ::encode( bl, bl_seq ) ;
        buffer_size = buffer_size + datasz ;
      }//if
    } //for

    std::cout << "buffer_size = " << buffer_size << std::endl ;

    // --------------------------------------------- //
    // build out FB_Meta
    // --------------------------------------------- //
    flatbuffers::FlatBufferBuilder *meta_builder = new flatbuffers::FlatBufferBuilder();
    Tables::createFbMeta( meta_builder, 
                          SFT_FLATBUF_UNION_COL,
                          reinterpret_cast<unsigned char*>( bl_seq.c_str() ),
                          bl_seq.length() ) ;

    // add meta_builder's data into a bufferlist as char*
    ceph::bufferlist meta_bl ;
    char* meta_builder_ptr = reinterpret_cast<char*>( meta_builder->GetBufferPointer() ) ;
    int meta_builder_size  = meta_builder->GetSize() ;
    std::cout << "meta_builder_size = " << meta_builder_size << std::endl ;
    meta_bl.append( meta_builder_ptr, meta_builder_size ) ;
    delete meta_builder;
    librados::bufferlist meta_wrapper_bl ;
    ::encode( meta_bl, meta_wrapper_bl ) ;
    size_t meta_wrapper_bl_sz = meta_wrapper_bl.length() ;
    std::cout << "meta_wrapper_bl_sz = " << meta_wrapper_bl_sz << std::endl ;

    // --------------------------------------------- //
    // do the write
    // --------------------------------------------- //
    // write bufferlist
    if( inputs.writeto == "ceph" )
      //writeToCeph( bl_seq, buffer_size, inputs.targetformat, inputs.targetoid, inputs.targetpool ) ;
      writeToCeph( meta_wrapper_bl, meta_builder_size, inputs.targetformat, inputs.targetoid, inputs.targetpool ) ;
    else if( inputs.writeto == "disk" )
      writeToDisk( bl_seq, buffer_size, inputs.targetformat, inputs.targetoid ) ;
    else {
      std::cout << ">>> unrecognized writeto '" << inputs.writeto << "'" << std::endl ;
      exit( 1 ) ;
    }

  } //cols

  // otherwise oops
  else {
    std::cout << ">> unrecognized inputs.write_type = '" << inputs.write_type << "'" << std::endl ;
  }
} // do_write

std::string getSchemaString( uint64_t ncols, 
                             std::vector< std::string > schema_attnames, 
                             std::vector< Tables::SkyDataType > schema_datatypes_sdt,
                             bool debug ) {
  std::string schema_string = "" ;
  for( unsigned int i = 0; i < ncols; i++ ) {
    std::string att_name = schema_attnames[ i ] ;
    auto att_type = schema_datatypes_sdt[ i ] ;
    std::string this_entry = " " + std::to_string( i ) + " " ;
    switch( att_type ) {
      case Tables::SDT_UINT64 :
        this_entry = this_entry + std::to_string( Tables::SDT_UINT64 ) ;
        break ;
      case Tables::SDT_FLOAT :
        this_entry = this_entry + std::to_string( Tables::SDT_FLOAT ) ;
        break ;
      case Tables::SDT_STRING :
        this_entry = this_entry + std::to_string( Tables::SDT_STRING ) ;
        break ;
      default :
        std::cout << ">>4 unrecognized att_type '" << att_type << "'" << std::endl ;
        exit( 1 ) ;
    } //switch
    this_entry = this_entry + " 0 0 " + att_name + " \n" ;
    schema_string = schema_string + this_entry ;
  } //for

  if( debug )
    std::cout << "schema_string = " << schema_string << std::endl ;

  return schema_string ;
} //getSchemaString


