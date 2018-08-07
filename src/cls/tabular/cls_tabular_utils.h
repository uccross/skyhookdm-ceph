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
#include "cls_tabular.h"
#include "flatbuffers/flexbuffers.h"
#include "skyhookv1_generated.h"


void dummyfunc(int val);

namespace Tables {

enum FbDataType {
    FbTypeInt = 1,
    FbTypeDouble,
    FbTypeChar,
    FbTypeDate,
    FbTypeString};
    
typedef flatbuffers::FlatBufferBuilder fbBuilder;
typedef flatbuffers::FlatBufferBuilder* fbb;
typedef flexbuffers::Builder flxBuilder;
typedef vector<uint8_t> delete_vector;
typedef vector<flatbuffers::Offset<Row>> rows_vector;    

// used for our each col in our schema def
struct col_info 
{
    int id;
    int type;  
    std::string name;
    col_info(int i, int t, std::string n) : id(i), type(t), name(n) {}
}; 

// the root table header contains db/table level metadata only
typedef struct 
{
    int skyhook_version;
    int schema_version;
    string table_name;
    string schema;
    delete_vector delete_vec;
    int rows_offset;
    int nrows;
}sky_root_header;

// the row table header contains row level metdata only
typedef struct 
{
    int64_t RID;
    vector<uint64_t> nullbits;
    int data_offset;	
}sky_row_header;

// TODO: this should be stored in omap of this object
// format of table's schema and query's return schema must be same.
// line1: represents partitioning key (used for obj id)
// line2+: col_num col_type col_name
const std::string table_schema_string = \
    " \
    orderkey linenumber \
    0 1 orderkey \
    1 1 partkey \
    2 1 suppkey \
    3 1 linenumber \
    4 2 quantity \
    5 2 extendedprice \
    6 2 discount \
    7 2 tax \
    8 3 returnflag \
    9 3 linestatus \
    10 4 shipdate \
    11 4 commitdate \
    12 4 receipdate \
    13 5 shipinstruct \
    14 5 shipmode \
    15 5 comment \
    ";

void printSkyRootHeader(sky_root_header *root);
void printSkyRows(Tables::sky_root_header *root);
inline std::vector<std::string> delimStringSplit(const std::string &s, char delim);
sky_root_header* getSkyRootHeader(const char * fb, size_t fb_size);
int getSchemaFormat(std::string schema_string, vector<col_info>& cols);

} // end namespace Tables

#endif
