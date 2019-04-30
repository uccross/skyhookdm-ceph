/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#ifndef CLS_TEST_UTILS_H
#define CLS_TEST_UTILS_H

#include <string>
#include <sstream>
#include <type_traits>
#include "include/types.h"
#include <errno.h>
#include <time.h>
#include "objclass/objclass.h"
#include "flatbuffers/flexbuffers.h"
#include "include/rados/librados.hpp"

#include "basic_generated.h"

#define checkret(r,v) do { \
  if (r != v) { \
    fprintf(stderr, "error %d/%s\n", r, strerror(-r)); \
    assert(0); \
    exit(1); \
  } } while (0)

void print_test() ;
void save_to_basic_fb( std::vector< uint64_t > ) ;
void read_basic_fb( std::string ) ;

#endif
