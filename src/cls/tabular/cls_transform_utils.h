/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#ifndef CLS_TABULAR_UTILS_H
#define CLS_TABULAR_UTILS_H

#include <string>
#include <sstream>
#include <type_traits>

#include "include/types.h"
#include <errno.h>
#include <time.h>
#include <boost/algorithm/string/classification.hpp> // for boost::is_any_of
#include <boost/algorithm/string/split.hpp> // for boost::split
#include <boost/algorithm/string.hpp> // for boost::trim
#include <boost/lexical_cast.hpp>

#include "re2/re2.h"
#include "objclass/objclass.h"

#include "cls_tabular.h"
#include "flatbuffers/flexbuffers.h"
#include "include/rados/librados.hpp"

//#include "transforms_generated.h"
#include "rows_generated.h"
#include "cols_int_generated.h"
#include "cols_float_generated.h"

#define checkret(r,v) do { \
  if (r != v) { \
    fprintf(stderr, "error %d/%s\n", r, strerror(-r)); \
    assert(0); \
    exit(1); \
  } } while (0)

struct spj_query_op {
  std::string oid ;
  std::string pool ;
  std::vector< std::string > select_atts ;
  std::vector< std::string > from_rels ;
  std::vector< std::string > where_preds ;
} ;

struct transform_op {
  std::string oid ;
  std::string pool ;
  std::string table_name ;

  // change these to ints
  std::string transform_type ;
  std::string layout ;
} ;

void execute_transform( transform_op ) ;
void execute_query( spj_query_op, std::string ) ;
librados::bufferlist transpose( librados::bufferlist ) ;
librados::bufferlist set_rows() ;

#endif
