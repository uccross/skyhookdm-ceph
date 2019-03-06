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

struct dataset_struct {
  std::vector<std::pair<std::string,int>> data ;
  std::vector< std::string > schema ;

  dataset_struct() {}

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
    s.append( "dataset_struct :" ) ;
    for( unsigned int i = 0; i < data.size(); i++ ) {
      auto apair = data[i] ;
      s.append( "  " ) ;
      s.append( apair.first ) ;
      s.append( ", " ) ;
      s.append( std::to_string( apair.second ) ) ;
    }
    return s ;
  }

  int getLength() {
    return data.size() ;
  }

} ;
WRITE_CLASS_ENCODER( dataset_struct )

class Dataset {

  int type_code = -1 ;
  int num_sets  = 0 ; // number of times set_dataset called on this dataset

  std::string                str_dataset         ; // 0
  std::vector< std::string > vect_str_dataset    ; // 1
  std::vector< uint64_t >    vect_uint64_dataset ; // 2

  std::vector< dataset_struct > vect_dataset_struct_dataset ; // 3

  public:
    Dataset( int tc ) {
      type_code = tc ;
    }

    void set_dataset( std::string a_str ) {
      assert( num_sets == 0 ) ;
      str_dataset = a_str ;
      num_sets++ ;
    }

    void set_dataset( std::vector< std::string > vect_str ) {
      assert( num_sets == 0 ) ;
      vect_str_dataset = vect_str ;
      num_sets++ ;
    }

    void set_dataset( std::vector< uint64_t > vect_uint64 ) {
      assert( num_sets == 0 ) ;
      vect_uint64_dataset = vect_uint64 ;
      num_sets++ ;
    }

    void set_dataset( std::vector< dataset_struct > dataset_struct ) {
      assert( num_sets == 0 ) ;
      vect_dataset_struct_dataset = dataset_struct ;
      num_sets++ ;
    }

    int get_type_code() {
      return type_code ;
    }

    int get_num_sets() {
      return num_sets ;
    }

    void print_dataset () {
      switch( type_code ) {
        case 0 : 
          std::cout << str_dataset << std::endl ;
          break ;
        case 1 : 
          std::cout << vect_str_dataset << std::endl ;
          break ;
        case 2 : 
          std::cout << vect_uint64_dataset << std::endl ;
          break ;
        case 3 : 
          std::cout << dataset_struct_toString() << std::endl ;
          break ;
      }
    }

    std::string dataset_struct_toString() {
      if( type_code == 3 ) {
        std::string total_str = "" ;
        for( int i = 0; i < vect_dataset_struct_dataset.size(); i++ ) {
          total_str = total_str + vect_dataset_struct_dataset[i].toString() + "\n" ;
        }
        return total_str ;
      }
      else {
        return "wha?" ;
      }
    }

} ;

#endif
