/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/

#ifndef FBWRITER_FB_UTILS_H
#define FBWRITER_FBU_UTILS_H

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

const uint8_t SKYHOOK_VERSION = 2 ;
const uint8_t SCHEMA_VERSION  = 1 ;

std::vector< std::string > parse_csv_str( std::string, char ) ;

struct linedata_t {

  std::vector< uint32_t > uint32s ;
  std::vector< uint64_t > uint64s ;
  std::vector< float >    floats ;
  std::vector< std::string > strs ;

  int uint32_idx ;
  int uint64_idx ;
  int float_idx ;
  int str_idx ;

  int id ;

  void parseLine( std::vector< Tables::SkyDataType > schema_datatypes_sdt, 
                  std::string line ) {

    //std::cout << "parsing : " << line << std::endl ;
    std::vector< std::string > line_vect = parse_csv_str( line, '|' ) ;
    for( unsigned int i = 0; i < line_vect.size(); i++ ) {
      Tables::SkyDataType atttype = schema_datatypes_sdt[i] ;
      std::string datum           = line_vect[i] ;

      switch( atttype ) {
        case Tables::SDT_UINT32 :
          uint32s.push_back( std::stoi( datum ) ) ;
          break ;
        case Tables::SDT_UINT64 :
          uint64s.push_back( std::stoi( datum ) ) ;
          break ;
        case Tables::SDT_FLOAT :
          floats.push_back( std::stof( datum ) ) ;
          break ;
        case Tables::SDT_STRING :
          strs.push_back( datum ) ;
          break ;
        default :
          std::cout << "\n>>3 unrecognized atttype '" << std::to_string( atttype ) << "'" << std::endl ;
          exit( 1 ) ;
      } //switch

    } //for
  } // parseLine

  void toString() {
    std::cout << "=============================================" << std::endl ;
    bool first = true ;
    for( unsigned int i = 0; i < uint32s.size(); i++ ) {
      if( !first ) std::cout << "|" ;
      else first = false ;
      std::cout << uint32s[i] ;
    }
    std::cout << std::endl ;
    std::cout << "=============================================" << std::endl ;
    first = true ;
    for( unsigned int i = 0; i < uint64s.size(); i++ ) {
      if( !first ) std::cout << "|" ;
      else first = false ;
      std::cout << uint64s[i] ;
    }
    std::cout << std::endl ;
    std::cout << "=============================================" << std::endl ;
    first = true ;
    for( unsigned int i = 0; i < floats.size(); i++ ) {
      if( !first ) std::cout << "|" ;
      else first = false ;
      std::cout << floats[i] ;
    }
    std::cout << std::endl ;
    std::cout << "=============================================" << std::endl ;
    first = true ;
    for( unsigned int i = 0; i < strs.size(); i++ ) {
      if( !first ) std::cout << "|" ;
      else first = false ;
      std::cout << strs[i] ;
    }
    std::cout << std::endl ;
  } // toString

  // constructor
  linedata_t( std::vector< Tables::SkyDataType > schema_datatypes_sdt, 
                           std::string line,
                           int i ) {

    uint32_idx = 0 ;
    uint64_idx = 0 ;
    float_idx  = 0 ;
    str_idx    = 0 ;

    id = i ;

    // parse
    parseLine( schema_datatypes_sdt, line ) ;
    // to string
    //toString() ;
  } ;

  uint32_t get_uint32() {
    uint32_t curr_uint32 = uint32s[ uint32_idx ] ;
    uint32_idx++ ;
    return curr_uint32 ;
  }

  uint64_t get_uint64() {
    uint64_t curr_uint64 = uint64s[ uint64_idx ] ;
    uint64_idx++ ;
    return curr_uint64 ;
  }

  float get_float() {
    float curr_float = floats[ float_idx ] ;
    float_idx++ ;
    return curr_float ;
  }

  std::string get_str() {
    std::string curr_str = strs[ str_idx ] ;
    str_idx++ ;
    return curr_str ;
  }
} ; //linedata_t

struct linecollection_t {
  std::vector< std::vector< uint32_t > > listof_int_vect_raw ;
  std::vector< std::vector< uint64_t > > listof_uint64_vect_raw ;
  std::vector< std::vector< float > > listof_float_vect_raw ;
  std::vector< std::vector< std::string > > listof_string_vect_raw_strs ;
  std::vector< std::vector< flatbuffers::Offset<flatbuffers::String> > > listof_string_vect_raw ;
  std::vector< std::vector< std::string > > indexer ;
  uint64_t int_vect_cnt ;
  uint64_t uint64_vect_cnt ;
  uint64_t float_vect_cnt ;
  uint64_t string_vect_cnt ;

  linecollection_t() {
    // collect and store linecollection
    // initialize struct with empty vects
    int_vect_cnt = 0 ;
    uint64_vect_cnt = 0 ;
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
    std::cout << "listof_uint64_vect_raw.size() = " << listof_uint64_vect_raw.size() << std::endl ;
    std::cout << "listof_uint64_vect_raw :" << std::endl ;
    std::cout << "----------------------" << std::endl ;
    for( unsigned int i = 0; i < listof_uint64_vect_raw.size(); i++ ) {
      std::cout << "listof_uint64_vect_raw[" << std::to_string( i ) << "].size() = " 
                << listof_uint64_vect_raw[i].size() << std::endl ;
      for( unsigned int j = 0; j < listof_uint64_vect_raw[i].size(); j++ ) {
        std::cout << listof_uint64_vect_raw[i][j] ;
        if( j < listof_uint64_vect_raw[i].size()-1 )
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
} ; // linecollection_t

struct cmdline_inputs_t {
  bool debug ;
  std::string write_type ;
  std::string filename ;
  std::string schema_datatypes ;
  std::string schema_attnames ;
  std::string schema_iskey ;
  std::string schema_isnullable ;
  std::string table_name ;
  uint64_t nrows ;
  uint64_t ncols ;
  uint64_t cols_per_fb ;
  uint64_t obj_counter ;
  std::string writeto ;
  std::string targetformat ;
  std::string targetoid ;
  std::string targetpool ;
} ;

int writeToDisk( librados::bufferlist wrapper_bl, 
                 int bufsz, 
                 std::string target_format, 
                 std::string target_oid,
                 std::string SAVE_DIR );

int writeToCeph( librados::bufferlist bl_seq, 
                 int bufsz, 
                 std::string target_format, 
                 std::string target_oid, 
                 std::string target_pool );

void do_write( cmdline_inputs_t, uint64_t, uint64_t, bool, std::string ) ;
void do_write2( cmdline_inputs_t inputs,
               uint64_t rid_start_value,
               uint64_t rid_end_value,
               bool debug,
               std::string SAVE_DIR,
               std::vector<std::string> csv_strs ) ;

std::vector< std::string > parse_csv_str( std::string instr, char delim ) ;

std::string get_schema_string( 
  int ncols, 
  std::vector< std::string > schema_iskey, 
  std::vector< std::string > schema_isnullable, 
  std::vector< std::string > schema_attnames, 
  std::vector< Tables::SkyDataType > schema_datatypes_sdt );


linecollection_t process_line_collection(
  flatbuffers::FlatBufferBuilder& builder,
  std::vector< linedata_t > line_collection,
  cmdline_inputs_t inputs,
  bool debug,
  std::vector< Tables::SkyDataType > schema_datatypes_sdt );

#endif
