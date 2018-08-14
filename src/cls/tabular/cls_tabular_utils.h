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

#include "include/types.h"

#include <boost/algorithm/string/classification.hpp> // Include boost::for is_any_of
#include <boost/algorithm/string/split.hpp> // Include for boost::split
#include <boost/algorithm/string.hpp> // include for boost::trim

#include "cls_tabular.h"
#include "flatbuffers/flexbuffers.h"
#include "skyhookv1_generated.h"

namespace Tables {

enum TablesErrCodes {
    EmptySchema = 1,
    BadColInfoFormat,
    BadColInfoConversion,
    UnsupportedFbDataType
};

typedef flatbuffers::FlatBufferBuilder fbBuilder;
typedef flatbuffers::FlatBufferBuilder* fbb;
typedef flexbuffers::Builder flxBuilder;
typedef vector<uint8_t> delete_vector;
typedef vector<flatbuffers::Offset<Row>> rows_vector;

// the root table header contains db/table level metadata only
typedef struct {
    int skyhook_version;
    int schema_version;
    string table_name;
    string schema;
    delete_vector delete_vec;
    int rows_offset;
    int nrows;
} sky_root_header;

// the row table header contains row level metdata only
typedef struct {
    int64_t RID;
    vector<uint64_t> nullbits;
    int data_offset;
} sky_row_header;

const int offset_to_skyhook_version = 4;
const int offset_to_schema_version = 6;
const int offset_to_table_name = 8;
const int offset_to_schema = 10;
const int offset_to_delete_vec = 12;
const int offset_to_rows_vec = 14;
const int offset_to_nrows = 16;
const int offset_to_RID = 4;
const int offset_to_nullbits_vec = 6;
const int offset_to_data = 8;

enum FbDataType {
    FbTypeInt = 1,  // note: must start at 1
    FbTypeDouble,
    FbTypeChar,
    FbTypeDate,
    FbTypeString,
    FbType_LAST  // note: must appear last for bounds check
};

// used for each col in our schema def
struct col_info {
    int id;
    int type;
    bool is_key;
    std::string name;

    col_info(int i, int t, bool b, std::string n) :
            id(i),
            type(t),
            is_key(b),
            name(n) {assert(type > 0 && type < FbDataType::FbType_LAST );}

    col_info(std::string i, std::string t, std::string b, std::string n) :
            id(atoi(i.c_str())),
            type(atoi(t.c_str())),
            is_key(b[0]=='1'),
            name(n) {assert(type > 0 && type < FbDataType::FbType_LAST );}
};

// TODO: the schema be stored in omap of the object, because it applies to all
// flatbuffers it contains, and all flatbufs in the object are updated
// atomically during any schema change so they always reflect the same schema.
// format: "col_id col_type col_is_key col_name \n"
const std::string lineitem_test_schema_string = " \
    0 1 1 orderkey \n\
    1 1 0 partkey \n\
    2 1 0 suppkey \n\
    3 1 1 linenumber \n\
    4 2 0 quantity \n\
    5 2 0 extendedprice \n\
    6 2 0 discount \n\
    7 2 0 tax \n\
    8 3 0 returnflag \n\
    9 3 0 linestatus \n\
    10 4 0 shipdate \n\
    11 4 0 commitdate \n\
    12 4 0 receipdate \n\
    13 5 0 shipinstruct \n\
    14 5 0 shipmode \n\
    15 5 0 comment \n\
    ";

void printSkyRootHeader(sky_root_header *root);
void printSkyRows(const char* fb, size_t fb_size,
        vector<struct col_info> &schema);
sky_root_header* getSkyRootHeader(const char * fb, size_t fb_size);
int getSchemaFormat(std::string schema_string, vector<col_info>& cols);
inline std::vector<std::string> delimStringSplit(const std::string &s, char delim);
int extractSchema(vector<struct col_info> &schema, string &schema_string);
} // end namespace Tables

#endif
