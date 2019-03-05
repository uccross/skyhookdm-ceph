/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/

#include <iostream>
#include <algorithm>
#include <atomic>
#include <thread>
#include <condition_variable>
#include "include/rados/librados.hpp"
#include "cls/tabular/cls_tabular.h"
#include "cls/tabular/cls_tabular_utils.h"
#include "re2/re2.h"

// other includes
#include "cls/split/cls_split.h"
#include "cls/transform/cls_transform.h"

struct vop_query {
  std::vector< std::string > attribute_vect ;
  std::string relation_name ;

  vop_query() {}

  // serialize the fields into bufferlist to be sent over the wire
  void encode( bufferlist& bl ) const {
    ENCODE_START( 1, 1, bl ) ;
    ::encode( attribute_vect, bl ) ;
    ::encode( relation_name, bl ) ;
    ENCODE_FINISH( bl ) ;
  }

  // deserialize the fields from the bufferlist into this struct
  void decode( bufferlist::iterator& bl ) {
    DECODE_START( 1, bl ) ;
    ::decode( attribute_vect, bl ) ;
    ::decode( relation_name, bl ) ;
    DECODE_FINISH( bl ) ;
  }

  std::string toString() {
    std::string att_str ;
    att_str.append( relation_name ) ;
    att_str.append( " : " ) ;
    for( unsigned int i = 0; i < attribute_vect.size(); i++ ) {
      if( i < attribute_vect.size() - 1 ) {
        att_str.append( attribute_vect[ i ] + "," ) ;
      }
      else {
        att_str.append( attribute_vect[ i ] ) ;
      }
    }
    return att_str ;
  }
} ;
WRITE_CLASS_ENCODER( vop_query )

void execute_query ( vop_query vq, librados::bufferlist relation ) ;
