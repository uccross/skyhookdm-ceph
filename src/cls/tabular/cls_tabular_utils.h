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
    UnsupportedSkyDataType,
    RequestedColIndexOOB
};

// skyhookdb data types, as supported by underlying data format
// TODO:  add more types supported natively by flatbuffers
enum SkyDataType {
    SkyTypeInt = 1,  // note: must start at 1
    SkyTypeDouble,
    SkyTypeChar,
    SkyTypeDate,
    SkyTypeString,
    SkyType_LAST  // note: must appear last for bounds check
};

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

// col metadata used for the schema
struct col_info {
    const int idx;
    const int type;
    const bool is_key;
    const bool nullable;
    const std::string name;

    col_info(int i, int t, bool key, bool nulls, std::string n) :
        idx(i),
        type(t),
        is_key(key),
        nullable(nulls),
        name(n) {assert(type > 0 && type < SkyDataType::SkyType_LAST );}

    col_info(std::string i, std::string t, std::string key, std::string nulls,
        std::string n) :
        idx(atoi(i.c_str())),
        type(atoi(t.c_str())),
        is_key(key[0]=='1'),
        nullable(nulls[0]=='1'),
        name(n) {assert(type > 0 && type < SkyDataType::SkyType_LAST );}

    std::string toString() {
        return ( "   " +
            std::to_string(idx) + " " +
            std::to_string(type) + " " +
            std::to_string(is_key) + " " +
            std::to_string(nullable) + " " +
            name + "   ");}

    bool compareName(std::string colname) {return (colname==name)?true:false;}
};
typedef vector<struct col_info> schema_vec;

// the below are used in our root table
typedef vector<uint8_t> delete_vector;
typedef const flatbuffers::Vector<flatbuffers::Offset<Row>>* row_offs;

// the below are used in our row table
typedef vector<uint64_t> nullbits_vector;
typedef flexbuffers::Reference row_data_ref;

// skyhookdb root metadata, refering to a (sub)partition of rows
// abstracts a partition from its underlying data format/layout
struct root_table {
    const int skyhook_version;
    int schema_version;
    string table_name;
    string schema;
    delete_vector delete_vec;
    row_offs offs;
    uint32_t nrows;

    root_table(int skyver, int schmver, std::string tblname, std::string schm,
               delete_vector d, row_offs ro, uint32_t n) :
        skyhook_version(skyver),
        schema_version(schmver),
        table_name(tblname),
        schema(schm),
        delete_vec(d),
        offs(ro),
        nrows(n) {};

};
typedef struct root_table sky_root_header;

// skyhookdb row metadata and row data, wraps a row of data
// abstracts a row from its underlying data format/layout
struct row_table {
    const int64_t RID;
    nullbits_vector nullbits;
    const row_data_ref data;

    row_table(int64_t rid, nullbits_vector n, row_data_ref  d) :
        RID(rid),
        nullbits(n),
        data(d) {};
};
typedef struct row_table sky_row_header;

// a test schema for the tpch lineitem table.
// format: "col_idx col_type col_is_key nullable col_name \n"
// note the col_idx always refers to the index in the table's current schema
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

// a test schema for procection over the tpch lineitem table.
// format: "col_idx col_type col_is_key nullable col_name \n"
// note the col_idx always refers to the index in the table's current schema
const std::string lineitem_test_project_schema_string = " \
    0 1 1 0 orderkey \n\
    1 1 0 1 partkey \n\
    3 1 1 0 linenumber \n\
    4 2 0 1 quantity \n\
    5 2 0 1 extendedprice \n\
    ";

// these extract the current data format (flatbuf) into the skyhookdb
// root table and row table data structure defined above, abstracting
// skyhookdb data partitions design from the underlying data format.
sky_root_header getSkyRootHeader(const char *fb, size_t fb_size);
sky_row_header getSkyRowHeader(const Tables::Row *rec);

void printSkyRootHeader(sky_root_header *r);
void printSkyRowHeader(sky_row_header *r);
void printSkyFb(const char* fb, size_t fb_size, schema_vec &schema);

void getSchemaFromProjectCols(schema_vec &ret_schema,
                              schema_vec &current_schema,
                              std::string project_col_names);
int getSchemaFromSchemaString(schema_vec &schema, std::string schema_string);
std::string getSchemaStrFromSchema(schema_vec schema);

// for proj, select(TODO), fastpath, aggregations(TODO), build return fb
int processSkyFb(
        flatbuffers::FlatBufferBuilder &flatb,
        schema_vec &schema_in,
        schema_vec &schema_out,
        const char *fb,
        const size_t fb_size,
        std::string &errmsg);

} // end namespace Tables

#endif
