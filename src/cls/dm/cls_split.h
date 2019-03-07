/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#ifndef CLS_SPLIT_H
#define CLS_SPLIT_H

#include "include/rados/librados.hpp"

// https://stackoverflow.com/questions/3247861/example-of-uuid-generation-using-boost-in-c
#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.
#include <boost/lexical_cast.hpp>
#include <tuple>
#include <string>

#include "include/types.h"

int ceph_split( librados::bufferlist* blist_in, 
                librados::bufferlist* blist_out0, 
                librados::bufferlist* blist_out1, 
                int type_code,
                int split_op_id,
                const char* split_pattern="" ) ;

int ceph_split_12( librados::bufferlist* blist_in, 
                   librados::bufferlist* blist_out0, 
                   librados::bufferlist* blist_out1,
                   int type_code ) ;

int ceph_split_pattern( librados::bufferlist* blist_in, 
                        librados::bufferlist* blist_out0, 
                        librados::bufferlist* blist_out1, 
                        const char* spit_pattern ) ;

int ceph_split_noop( librados::bufferlist* blist_in, 
                     librados::bufferlist* blist_out0, 
                     librados::bufferlist* blist_out1 ) ;

#endif
