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
typedef vector<uint64_t> nullbits_vector;
typedef vector<flatbuffers::Offset<Row>> row_offsets_vector;
typedef vector<const Row*> rows_vector;
typedef flexbuffers::Vector flx_vector;

// the root table header contains db/table level metadata only
struct root_table {
    const int skyhook_version;
    int schema_version;
    string table_name;
    string schema;
    delete_vector delete_vec;
    const rows_vector rows;
    int nrows;

    root_table(int skyver, int schmver, std::string tblname, std::string schm,
                delete_vector d, rows_vector r, int n) :
        skyhook_version(skyver),
        schema_version(schmver),
        table_name(tblname),
        schema(schm),
        delete_vec(d),
        rows(r),
        nrows(n) {};

};
typedef struct root_table sky_root_header;

// the row table header contains row level metdata only
struct row_table {
    const int64_t RID;
    nullbits_vector nullbits;
    const flexbuffers::Reference data;

    row_table(int64_t rid, nullbits_vector n, flexbuffers::Reference  d) :
        RID(rid),
        nullbits(n),
        data(d) {};
};
typedef struct row_table sky_row_header;

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
    bool nullable;
    std::string name;

    col_info(int i, int t, bool key, bool nulls, std::string n) :
            id(i),
            type(t),
            is_key(key),
            nullable(nulls),
            name(n) {assert(type > 0 && type < FbDataType::FbType_LAST );}

    col_info(std::string i, std::string t, std::string key, std::string nulls,
             std::string n) :
            id(atoi(i.c_str())),
            type(atoi(t.c_str())),
            is_key(key[0]=='1'),
            nullable(nulls[0]=='1'),
            name(n) {assert(type > 0 && type < FbDataType::FbType_LAST );}
};

// TODO: the schema be stored in omap of the object, because it applies to all
// flatbuffers it contains, and all flatbufs in the object are updated
// atomically during any schema change so they always reflect the same schema.
// format: "col_id col_type col_is_key nullable col_name \n"
const std::string lineitem_test_schema_string = " \
    0 1 1 0 orderkey \n\
    1 1 0 1 partkey \n\
    2 1 0 1 suppkey \n\
    3 1 1 0 linenumber \n\
    4 2 0 1 quantity \n\
    5 2 0 1 extendedprice \n\
    6 2 0 1 discount \n\
    7 2 0 1 tax \n\
    8 3 0 1 returnflag \n\
    9 3 0 1 linestatus \n\
    10 4 0 1 shipdate \n\
    11 4 0 1 commitdate \n\
    12 4 0 1 receipdate \n\
    13 5 0 1 shipinstruct \n\
    14 5 0 1 shipmode \n\
    15 5 0 1 comment \n\
    ";

sky_root_header getSkyRootHeader(const char *fb, size_t fb_size);
sky_row_header getSkyRowHeader(const Tables::Row *rec);

void printSkyRootHeader(sky_root_header *r);
void printSkyRowHeader(sky_row_header *r);

void printSkyFb(const char* fb, size_t fb_size,
        vector<struct col_info> &schema);

int getSchemaFormat(std::string schema_string, vector<col_info>& cols);
inline std::vector<std::string> delimStringSplit(const std::string &s, char delim);
int extractSchema(vector<struct col_info> &schema, string &schema_string);
} // end namespace Tables

#endif
