/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#ifndef CLS_TABULAR_PROCESSING_H
#define CLS_TABULAR_PROCESSING_H

#include <string>
#include <sstream>

#include "cls_tabular_utils.h"
#include "cls_tabular.h"


// Processing code for different data formats, since each has their own API
// Also processing for WASM, which requires arguments of C types, not C++


namespace Tables {


// process flatbuffer format data blob
int processSkyFb(
        flatbuffers::FlatBufferBuilder& flatb,
        schema_vec& data_schema,
        schema_vec& query_schema,
        predicate_vec& preds,
        const char* fb,
        const size_t fb_size,
        std::string& errmsg,
        const std::vector<uint32_t>& row_nums=std::vector<uint32_t>());

// process arrow format data blob, col access style
int processArrowCol(
        std::shared_ptr<arrow::Table>* table,
        schema_vec& tbl_schema,
        schema_vec& query_schema,
        predicate_vec& preds,
        const char* dataptr,
        const size_t datasz,
        std::string& errmsg,
        const std::vector<uint32_t>& row_nums=std::vector<uint32_t>());

// process arrow format data blob, row access style
int processArrow(
        std::shared_ptr<arrow::Table>* table,
        schema_vec& tbl_schema,
        schema_vec& query_schema,
        predicate_vec& preds,
        const char* dataptr,
        const size_t datasz,
        std::string& errmsg,
        const std::vector<uint32_t>& row_nums=std::vector<uint32_t>());

// process flatbuffer format data blob with wasm
int processSkyFbWASM(
        char* _flatbldr,
        int _flatbldr_len,
        char* _data_schema,
        int _data_schema_len,
        char* _query_schema,
        int _query_schema_len,
        char* _preds,
        int _preds_len,
        char* _fb,
        int _fb_size,
        char* _errmsg,
        int _errmsg_len,
        int* _row_nums,
        int _row_nums_size);

} // end namespace Tables


#endif
