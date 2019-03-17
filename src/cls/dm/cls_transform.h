/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#ifndef CLS_TRANSFORM_H
#define CLS_TRANSFORM_H

#include "include/rados/librados.hpp"

// https://stackoverflow.com/questions/3247861/example-of-uuid-generation-using-boost-in-c
#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.
#include <boost/lexical_cast.hpp>
#include <tuple>
#include <string>

#include "include/types.h"

int ceph_transform( librados::bufferlist* blist_in,
                    librados::bufferlist* blist_out,
                    int ceph_transform_op_id ) ;

int ceph_transform_id( librados::bufferlist* blist_in,
                       librados::bufferlist* blist_out ) ;

int ceph_transform_reverse( librados::bufferlist* blist_in,
                            librados::bufferlist* blist_out ) ;

int ceph_transform_sort( librados::bufferlist* blist_in,
                         librados::bufferlist* blist_out ) ;

int ceph_transform_transpose( librados::bufferlist* blist_in,
                              librados::bufferlist* blist_out ) ;

int ceph_transform_noop( librados::bufferlist* blist_in,
                         librados::bufferlist* blist_out ) ;


#endif
