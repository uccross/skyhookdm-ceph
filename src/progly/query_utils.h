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
#include "re2/re2.h"

#include "cls/tabular/cls_transform_utils.h"
#include "cls/tabular/cls_tabular.h"
#include "cls/tabular/cls_tabular_utils.h"
#include "include/rados/librados.hpp"

int process_fb_union( flatbuffers::FlatBufferBuilder&,
                      ceph::bufferlist,
                      std::vector<std::string> ) ;
