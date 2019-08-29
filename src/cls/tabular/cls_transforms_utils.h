/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#ifndef CLS_TRANSFORMS_UTILS_H
#define CLS_TRANSFORMS_UTILS_H

#include <string>
#include <sstream>
#include <type_traits>
#include <bitset>

#include "include/types.h"
#include <errno.h>
#include <time.h>
#include <boost/algorithm/string/classification.hpp> // for boost::is_any_of
#include <boost/algorithm/string/split.hpp> // for boost::split
#include <boost/algorithm/string.hpp> // for boost::trim
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/lexical_cast.hpp>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/ipc/reader.h>

#include "re2/re2.h"
#include "objclass/objclass.h"

#include "flatbuffers/flexbuffers.h"
#include "skyhookv2_generated.h"
#include "skyhookv2_csv_generated.h"
#include "fb_meta_generated.h"
#include "fbu_generated.h"

#include "fbwriter_fbu_utils.h"
#include "cls_transforms.h"

namespace Tables {

// Transform functions
int transform_fburows_to_fbucols(
        const char* fb,
        const size_t fb_size,
        std::string& errmsg,
        flatbuffers::FlatBufferBuilder& flatbldr,
        std::vector<int> project_cols);

// tools for fburows to fbucols transforms
std::string get_schema_data_types_fbu(std::string data_schema, std::vector<int> project_cols);
std::string get_schema_attnames_fbu(std::string data_schema, std::vector<int> project_cols);
std::string get_schema_iskey_fbu(std::string data_schema, std::vector<int> project_cols);
std::string get_schema_isnullable_fbu(std::string data_schema, std::vector<int> project_cols);

} // end of namespace

#define checkret(r,v) do { \
  if (r != v) { \
    fprintf(stderr, "error %d/%s\n", r, strerror(-r)); \
    assert(0); \
    exit(1); \
  } } while (0)

#endif
