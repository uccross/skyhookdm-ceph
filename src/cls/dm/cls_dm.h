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

  void append( std::vector<std::pair<std::string,int>> i ) {
    data.push_back( i ) ;
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

  int getLength() {
    return data.size() ;
  }

  void append( std::string i ) {
    data = data + i ;
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

  int getLength() {
    return data.size() ;
  }

  void append( std::string i ) {
    data.push_back( i ) ;
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

  int getLength() {
    return data.size() ;
  }

  void append( uint64_t i ) {
    data.push_back( i ) ;
  }
} ;
WRITE_CLASS_ENCODER( dataset_vect_uint64 )

struct Dataset {
  dataset_str         ds_str         ; // 0
  dataset_vect_str    ds_vect_str    ; // 1
  dataset_vect_uint64 ds_vect_uint64 ; // 2
  vvop                ds_vvop        ; // 3
  int type_code = -1 ;

  // ============================================ //
  //                 CONSTRUCTORS                 //
  // ============================================ //
  Dataset() {}

  Dataset( int tc ) {
    type_code = tc ;
  }

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
    ::encode( ds_vvop, bl ) ;
    ::encode( type_code, bl ) ;
    ENCODE_FINISH( bl ) ;
  }

  // deserialize the fields from the bufferlist into this struct
  void decode( bufferlist::iterator& bl ) {
    DECODE_START( 1, bl ) ;
    ::decode( ds_str, bl ) ;
    ::decode( ds_vect_str, bl ) ;
    ::decode( ds_vect_uint64, bl ) ;
    ::decode( ds_vvop, bl ) ;
    ::decode( type_code, bl ) ;
    DECODE_FINISH( bl ) ;
  }

  // ============================================ //
  //                  TOSTRING                    //
  // ============================================ //
  std::string toString() {
    std::string s ;

    switch( type_code ) {
      case 0 :
        s.append( ds_str.toString() ) ;
        break ;
      case 1 :
        s.append( ds_vect_str.toString() ) ;
        break ;
      case 2 :
        s.append( ds_vect_uint64.toString() ) ;
        break ;
      case 3 :
        s.append( ds_vvop.toString() ) ;
        break ;
      default :
        s.append( "cannot print string. unknow type_code '" + 
                  std::to_string( type_code ) + "'" ) ;
        break ;
    }

    return s ;
  }

  // ============================================ //
  //                  GET LENGTH                  //
  // ============================================ //
  int getLength() {
    int len = -1 ;
    switch( type_code ) {
      case 0 :
        len = ds_str.getLength() ;
        break ;
      case 1 :
        len = ds_vect_str.getLength() ;
        break ;
      case 2 :
        len = ds_vect_uint64.getLength() ;
        break ;
      case 3 :
        len = ds_vvop.getLength() ;
        break ;
      default :
        std::cout << "cannot print string. unknow type_code '" <<
                  std::to_string( type_code ) << "'" ;
        break ;
    }
    return len ;
  }

  // ============================================ //
  //                  APPEND                      //
  // ============================================ //
  void append( std::string i ) {
    switch( type_code ) {
      case 0 :
        ds_str.append( i ) ;
        break ;
      case 1 :
        ds_vect_str.append( i ) ;
        break ;
    }
  }

  void append( uint64_t i ) {
    ds_vect_uint64.append( i ) ;
  }

  void append( std::vector<std::pair<std::string,int>> i ) {
    ds_vvop.append( i ) ;
  }

  // ============================================ //
  //                  GET                         //
  // ============================================ //
  std::string get( int i ) {
    switch( type_code ) {
      case 0 :
        return string() + ds_str.data[i] ;
      case 1 : 
        return ds_vect_str.data[i] ;
    }
  }

  uint64_t get_uint64( int i ) {
    return ds_vect_uint64.data[i] ;
  }

  std::vector< std::pair<std::string,int> > get_vvop( int i ) {
    return ds_vvop.data[i] ;
  }

} ;
WRITE_CLASS_ENCODER( Dataset )

int get_index( Dataset, std::string ) ;
int get_index( Dataset, uint64_t ) ;
int get_index( dataset_str, std::string ) ;
int get_index( dataset_vect_str, std::string ) ;
int get_index( dataset_vect_uint64, uint64_t ) ;
int get_index( vvop, std::string ) ;

int get_midpoint( Dataset ) ;
int get_midpoint( dataset_str ) ;
int get_midpoint( dataset_vect_str ) ;
int get_midpoint( dataset_vect_uint64 ) ;
int get_midpoint( vvop ) ;

Dataset get_range( Dataset, int, int ) ;
Dataset get_range( dataset_str, int, int ) ;
Dataset get_range( dataset_vect_str, int, int ) ;
Dataset get_range( dataset_vect_uint64, int, int ) ;
Dataset get_range( vvop, int, int ) ;

#endif
