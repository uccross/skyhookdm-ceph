/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#ifndef CLS_DM_H
#define CLS_DM_H

#include "include/rados/librados.hpp"

// https://stackoverflow.com/questions/3247861/example-of-uuid-generation-using-boost-in-c
#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.
#include <boost/lexical_cast.hpp>
#include <tuple>
#include <string>

#include "include/types.h"


struct vvop {
  std::vector< std::vector<std::pair<std::string,int>> > data ;
  std::vector< std::string > schema ;

  vvop() {}

  // serialize the fields into bufferlist to be sent over the wire
  void encode( bufferlist& bl ) const {
    ENCODE_START( 1, 1, bl ) ;
    ::encode( data, bl ) ;
    ::encode( schema, bl ) ;
    ENCODE_FINISH( bl ) ;
  }

  // deserialize the fields from the bufferlist into this struct
  void decode( bufferlist::iterator& bl ) {
    DECODE_START( 1, bl ) ;
    ::decode( data, bl ) ;
    ::decode( schema, bl ) ;
    DECODE_FINISH( bl ) ;
  }

  std::string toString() {
    std::string s ;
    for( unsigned int i = 0; i < data.size(); i++ ) {
      s.append( "row:" ) ;
      for( unsigned int j = 0; j < data[i].size(); j++ ) {
        auto apair = data[i][j] ;
        s.append( "  " ) ;
        s.append( apair.first ) ;
        s.append( "," ) ;
        s.append( std::to_string( apair.second ) ) ;
      }
      s.append( "\n" ) ;
    }
    return s ;
  }

  int getLength() {
    return data.size() ;
  }

} ;
WRITE_CLASS_ENCODER( vvop )

struct dataset_str {
  std::string data ;

  dataset_str() {}

  // serialize the fields into bufferlist to be sent over the wire
  void encode( bufferlist& bl ) const {
    ENCODE_START( 1, 1, bl ) ;
    ::encode( data, bl ) ;
    ENCODE_FINISH( bl ) ;
  }

  // deserialize the fields from the bufferlist into this struct
  void decode( bufferlist::iterator& bl ) {
    DECODE_START( 1, bl ) ;
    ::decode( data, bl ) ;
    DECODE_FINISH( bl ) ;
  }

  std::string toString() {
    std::string s ;
    s.append( data ) ;
    return s ;
  }

} ;
WRITE_CLASS_ENCODER( dataset_str )

struct dataset_vect_str {
  std::vector< std::string > data ;

  dataset_vect_str() {}

  // serialize the fields into bufferlist to be sent over the wire
  void encode( bufferlist& bl ) const {
    ENCODE_START( 1, 1, bl ) ;
    ::encode( data, bl ) ;
    ENCODE_FINISH( bl ) ;
  }

  // deserialize the fields from the bufferlist into this struct
  void decode( bufferlist::iterator& bl ) {
    DECODE_START( 1, bl ) ;
    ::decode( data, bl ) ;
    DECODE_FINISH( bl ) ;
  }

  std::string toString() {
    std::string s ;
    for( unsigned int i = 0; i < data.size(); i++ ) {
      s.append( data[i] ) ;
      if( i < data.size() - 1 ) {
        s.append( "\n" ) ;
      }
    }
    return s ;
  }
} ;
WRITE_CLASS_ENCODER( dataset_vect_str )

struct dataset_vect_uint64 {
  std::vector< uint64_t > data ;

  dataset_vect_uint64() {}

  // serialize the fields into bufferlist to be sent over the wire
  void encode( bufferlist& bl ) const {
    ENCODE_START( 1, 1, bl ) ;
    ::encode( data, bl ) ;
    ENCODE_FINISH( bl ) ;
  }

  // deserialize the fields from the bufferlist into this struct
  void decode( bufferlist::iterator& bl ) {
    DECODE_START( 1, bl ) ;
    ::decode( data, bl ) ;
    DECODE_FINISH( bl ) ;
  }

  std::string toString() {
    std::string s ;
    for( unsigned int i = 0; i < data.size(); i++ ) {
      s.append( std::to_string( data[i] ) ) ;
      if( i < data.size() - 1 ) {
        s.append( "\n" ) ;
      }
    }
    return s ;
  }

} ;
WRITE_CLASS_ENCODER( dataset_vect_uint64 )

struct Dataset {
  dataset_str         ds_str         ; // 0
  dataset_vect_str    ds_vect_str    ; // 1
  dataset_vect_uint64 ds_vect_uint64 ; // 2
  vvop                 ds_vvop       ; // 3
  int type_code = -1 ;

  // ============================================ //
  //                 CONSTRUCTORS                 //
  // ============================================ //
  Dataset() {}

  Dataset( std::string data_in ) {
    ds_str.data = data_in ;
    type_code   = 0 ;
  }

  Dataset( std::vector< std::string > data_in ) {
    ds_vect_str.data = data_in ;
    type_code        = 1 ;
  }

  Dataset( std::vector< uint64_t > data_in ) {
    ds_vect_uint64.data = data_in ;
    type_code        = 2 ;
  }

  Dataset( std::vector<std::vector<std::pair<std::string,int>>> data_in,
           std::vector< std::string > schema_in ) {
    ds_vvop.data   = data_in ;
    ds_vvop.schema = schema_in ;
    type_code      = 3 ;
  }

  // ============================================ //
  //                 CEPH SERIALIZE               //
  // ============================================ //
  // serialize the fields into bufferlist to be sent over the wire
  void encode( bufferlist& bl ) const {
    ENCODE_START( 1, 1, bl ) ;
    ::encode( ds_str, bl ) ;
    ::encode( ds_vect_str, bl ) ;
    ::encode( ds_vect_uint64, bl ) ;
    ENCODE_FINISH( bl ) ;
  }

  // deserialize the fields from the bufferlist into this struct
  void decode( bufferlist::iterator& bl ) {
    DECODE_START( 1, bl ) ;
    ::decode( ds_str, bl ) ;
    ::decode( ds_vect_str, bl ) ;
    ::decode( ds_vect_uint64, bl ) ;
    DECODE_FINISH( bl ) ;
  }

  // ============================================ //
  //                  TOSTRING                    //
  // ============================================ //
  std::string toString() {
    std::string s ;

    // string
    if( type_code == 0 ) {
      s.append( ds_str.toString() ) ;
    }

    // vector< string >
    else if( type_code == 1 ) {
      s.append( ds_vect_str.toString() ) ;
    }

    // vector< uint64 >
    else if( type_code == 2 ) {
      s.append( ds_vect_uint64.toString() ) ;
    }

    // vvop
    else if( type_code == 3 ) {
      s.append( ds_vvop.toString() ) ;
    }

    return s ;
  }
} ;
WRITE_CLASS_ENCODER( Dataset )

int get_midpoint( Dataset ) ;
int get_midpoint( dataset_str ) ;
int get_midpoint( dataset_vect_str ) ;
int get_midpoint( dataset_vect_uint64 ) ;
int get_midpoint( vvop ) ;

std::pair< Dataset, Dataset > get_range( Dataset, int, int ) ;
std::pair< Dataset, Dataset > get_range( dataset_str, int, int ) ;
std::pair< Dataset, Dataset > get_range( dataset_vect_str, int, int ) ;
std::pair< Dataset, Dataset > get_range( dataset_vect_uint64, int, int ) ;
std::pair< Dataset, Dataset > get_range( vvop, int, int ) ;

#endif
