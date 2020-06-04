/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#include <errno.h>
#include <string>
#include <sstream>
#include <boost/lexical_cast.hpp>
#include <time.h>
#include "re2/re2.h"
#include "include/types.h"
#include "objclass/objclass.h"
#include "cls_tabular_utils.h"
#include "cls_tabular.h"

#include "../wasm/wasm_export.h"
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <fstream>

CLS_VER(1,0)
CLS_NAME(tabular)

cls_handle_t h_class;
cls_method_handle_t h_exec_query_op;
cls_method_handle_t h_example_query_op;
cls_method_handle_t h_test_query_op;
cls_method_handle_t h_hep_query_op;
cls_method_handle_t h_wasm_query_op;
cls_method_handle_t h_exec_runstats_op;
cls_method_handle_t h_build_index;
cls_method_handle_t h_exec_build_sky_index_op;
cls_method_handle_t h_transform_db_op;
cls_method_handle_t h_freelockobj_query_op;
cls_method_handle_t h_inittable_group_obj_query_op;
cls_method_handle_t h_getlockobj_query_op;
cls_method_handle_t h_acquirelockobj_query_op;
cls_method_handle_t h_createlockobj_query_op;


int bldr_size = 1024;
flatbuffers::FlatBufferBuilder result_builder(bldr_size);
char* meta_blob_data;
int meta_blob_size;

char* errmsg_ptr = (char*) calloc(256, sizeof(char));


void cls_log_message(std::string msg, bool is_err = false, int log_level = 20) {
    if (is_err)
        CLS_ERR("skyhook: %s", msg.c_str());
    else
        CLS_LOG(log_level,"skyhook: %s", msg.c_str());
}

static inline uint64_t __getns(clockid_t clock)
{
  struct timespec ts;
  int ret = clock_gettime(clock, &ts);
  assert(ret == 0);
  return (((uint64_t)ts.tv_sec) * 1000000000ULL) + ts.tv_nsec;
}

static inline uint64_t getns()
{
  return __getns(CLOCK_MONOTONIC);
}

// extract bytes as string for regex matching
static std::string string_ncopy(const char* buffer, std::size_t buffer_size) {
  const char* copyupto = std::find(buffer, buffer + buffer_size, 0);
  return std::string(buffer, copyupto);
}

extern "C" {
  //char *bh_read_file_to_buffer(const char *filename, int *ret_size);
  void _bh_log() {}
  int foo(wasm_exec_env_t exec_env , int a, int b)
{
    CLS_LOG(20,"In foo function");
    return a+b;
}

void foo2(wasm_exec_env_t exec_env, 
          uint32_t msg_offset, 
          uint32_t buffer_offset, 
          int32_t buf_len)
{
    wasm_module_inst_t module_inst = get_module_inst(exec_env);
    char *buffer;
    char * msg ;

    // do boundary check
    if (!wasm_runtime_validate_app_str_addr(module_inst, msg_offset))
        return;
    
    if (!wasm_runtime_validate_app_addr(module_inst,buffer_offset, buf_len))
        return;

    // do address conversion
    buffer = (char *)wasm_runtime_addr_app_to_native(module_inst, buffer_offset);
    msg = (char *) wasm_runtime_addr_app_to_native(module_inst, msg_offset);

    strncpy(buffer, msg, buf_len);
}

int processSkyFbWASM2(
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
        int _row_nums_size)
{
    

    using namespace Tables;
    
    _flatbldr = reinterpret_cast<char*>(&result_builder);
    flatbuffers::FlatBufferBuilder *flatbldr = (flatbuffers::FlatBufferBuilder*) _flatbldr;

    // query params strings to skyhook types.
    schema_vec data_schema = schemaFromString(std::string(_data_schema, _data_schema_len));
    std::string s = schemaToString(data_schema);
    
    schema_vec query_schema = schemaFromString(std::string(_query_schema, _query_schema_len));
    std::string st = schemaToString(query_schema);
    
    predicate_vec preds = predsFromString(data_schema, std::string(_preds, _preds_len));
    //std::string predsToString(predicate_vec &preds, schema_vec &schema) {
    std::string st2 = predsToString(preds,data_schema);

    _fb = meta_blob_data;
    _fb_size = meta_blob_size;
    _errmsg = errmsg_ptr;

    // this is the in-mem data (as a flatbuffer)
    const char* fb = _fb;
    int fb_size = _fb_size;

    std::string errmsg;

    // row_nums array represents those found by an index, if any.
    std::vector<uint32_t> row_nums(&_row_nums[0], &_row_nums[_row_nums_size]);

    int errcode = 0;
    delete_vector dead_rows;
    std::vector<flatbuffers::Offset<Tables::Record>> offs;
    sky_root root = getSkyRoot(fb, fb_size, SFT_FLATBUF_FLEX_ROW);
    

    int col_idx_max = -1;
    for (auto it=data_schema.begin(); it!=data_schema.end(); ++it) {
        if (it->idx > col_idx_max)
            col_idx_max = it->idx;
    }

    bool project_all = std::equal(data_schema.begin(), data_schema.end(),
                                  query_schema.begin(), compareColInfo);

    // build the flexbuf with computed aggregates, aggs are computed for
    // each row that passes, and added to flexbuf after loop below.
    bool encode_aggs = false;
    if (hasAggPreds(preds)) encode_aggs = true;
    bool encode_rows = !encode_aggs;

    // determines if we process specific rows (from index lookup) or all rows
    bool process_all_rows = false;
    uint32_t nrows = 0;
    if (row_nums.empty()) {         // default is empty, so process them all
        process_all_rows = true;
        nrows = root.nrows;
    }
    else {
        process_all_rows = false;  // process only specific rows
        nrows = row_nums.size();
    }

    for (uint32_t i = 0; i < nrows; i++){
        // process row i or the specified row number
        uint32_t rnum = 0;
        if (process_all_rows) rnum = i;
        else rnum = row_nums[i];

        if (rnum > root.nrows) {
            errmsg += "ERROR: rnum(" + std::to_string(rnum) +
                      ") > root.nrows(" + to_string(root.nrows) + ")";
            //  copy finite len errmsg into caller's char ptr
            size_t len = _errmsg_len;
            if(errmsg.length()+1 < len)
                len = errmsg.length()+1;
            strncpy(_errmsg, errmsg.c_str(), len);
            return RowIndexOOB;
        }

        // skip dead rows.
        if (root.delete_vec[rnum] == 1) continue;
        sky_rec rec = getSkyRec(static_cast<row_offs>(root.data_vec)->Get(rnum));
        
        // apply predicates to this record
        if (!preds.empty()) {
            bool pass = applyPredicates(preds, rec);
            if (!pass) continue;  // skip non matching rows.
        }        
        
        
        if (!encode_rows) continue;

        if (project_all) {
            // TODO:  just pass through row table offset to new data_vec
            // (which is also type offs), do not rebuild row table and flexbuf
        }
        
        auto row = rec.data.AsVector();

        flexbuffers::Builder *flexbldr = new flexbuffers::Builder();
        flatbuffers::Offset<flatbuffers::Vector<unsigned char>> datavec;

        flexbldr->Vector([&]() {

            // iter over the query schema, locating it within the data schema
            for (auto it=query_schema.begin();
                      it!=query_schema.end() && !errcode; ++it) {
                col_info col = *it;
                
                if (col.idx < AGG_COL_LAST or col.idx > col_idx_max) {
                    errcode = TablesErrCodes::RequestedColIndexOOB;
                    errmsg.append("ERROR processSkyFb(): table=" +
                            root.table_name + "; rid=" +
                            std::to_string(rec.RID) + " col.idx=" +
                            std::to_string(col.idx) + " OOB.");

                } else {

                    switch(col.type) {  // encode data val into flexbuf

                        case SDT_INT8:
                            flexbldr->Add(row[col.idx].AsInt8());
                            break;
                        case SDT_INT16:
                            flexbldr->Add(row[col.idx].AsInt16());
                            break;
                        case SDT_INT32:
                            flexbldr->Add(row[col.idx].AsInt32());
                            break;
                        case SDT_INT64:
                            flexbldr->Add(row[col.idx].AsInt64());
                            break;
                        case SDT_UINT8:
                            flexbldr->Add(row[col.idx].AsUInt8());
                            break;
                        case SDT_UINT16:
                            flexbldr->Add(row[col.idx].AsUInt16());
                            break;
                        case SDT_UINT32:
                            flexbldr->Add(row[col.idx].AsUInt32());
                            break;
                        case SDT_UINT64:
                            flexbldr->Add(row[col.idx].AsUInt64());
                            break;
                        case SDT_CHAR:
                            flexbldr->Add(row[col.idx].AsInt8());
                            break;
                        case SDT_UCHAR:
                            flexbldr->Add(row[col.idx].AsUInt8());
                            break;
                        case SDT_BOOL:
                            flexbldr->Add(row[col.idx].AsBool());
                            break;
                        case SDT_FLOAT:
                            flexbldr->Add(row[col.idx].AsFloat());
                            break;
                        case SDT_DOUBLE:
                            flexbldr->Add(row[col.idx].AsDouble());
                            break;
                        case SDT_DATE:
                            flexbldr->Add(row[col.idx].AsString().str());
                            break;
                        case SDT_STRING:
                            flexbldr->Add(row[col.idx].AsString().str());
                            break;
                        default: {
                            errcode = TablesErrCodes::UnsupportedSkyDataType;
                            errmsg.append("ERROR processSkyFb(): table=" +
                                    root.table_name + "; rid=" +
                                    std::to_string(rec.RID) + " col.type=" +
                                    std::to_string(col.type) +
                                    " UnsupportedSkyDataType.");
                        }
                    }
                }
            }
        });
        
        flexbldr->Finish();
        long long int row_limit;
        std::atomic<long long int> row_counter;
        row_counter += printFlatbufFlexRowAsCsv(
                    fb,
                    fb_size,
                    true,
                    true,
                    row_limit - row_counter);
        
        auto row_data = flatbldr->CreateVector(flexbldr->GetBuffer());
        delete flexbldr;

        // TODO: update nullbits
        auto nullbits = flatbldr->CreateVector(rec.nullbits);
        flatbuffers::Offset<Tables::Record> row_off = \
                Tables::CreateRecord(*flatbldr, rec.RID, nullbits, row_data);

        // Continue building the ROOT flatbuf's dead vector and rowOffsets vec
        dead_rows.push_back(0);
        offs.push_back(row_off);
        
        errmsg.append("ERROR processSkyFb() : !encode_aggs");

    }

    if (encode_aggs){
        PredicateBase* pb;
        flexbuffers::Builder *flexbldr = new flexbuffers::Builder();
        flexbldr->Vector([&]() {
            for (auto itp = preds.begin(); itp != preds.end(); ++itp) {

                // assumes preds appear in same order as return schema
                if (!(*itp)->isGlobalAgg()) continue;
                pb = *itp;
                switch(pb->colType()) {  // encode agg data val into flexbuf
                    case SDT_INT64: {
                        TypedPredicate<int64_t>* p = \
                                dynamic_cast<TypedPredicate<int64_t>*>(pb);
                        int64_t agg_val = p->Val();
                        flexbldr->Add(agg_val);
                        p->updateAgg(0);  // reset accumulated add val
                        break;
                    }
                    case SDT_UINT32: {
                        TypedPredicate<uint32_t>* p = \
                                dynamic_cast<TypedPredicate<uint32_t>*>(pb);
                        uint32_t agg_val = p->Val();
                        flexbldr->Add(agg_val);
                        p->updateAgg(0);  // reset accumulated add val
                        break;
                    }
                    case SDT_UINT64: {
                        TypedPredicate<uint64_t>* p = \
                                dynamic_cast<TypedPredicate<uint64_t>*>(pb);
                        uint64_t agg_val = p->Val();
                        flexbldr->Add(agg_val);
                        p->updateAgg(0);  // reset accumulated add val
                        break;
                    }
                    case SDT_FLOAT: {
                        TypedPredicate<float>* p = \
                                dynamic_cast<TypedPredicate<float>*>(pb);
                        float agg_val = p->Val();
                        flexbldr->Add(agg_val);
                        p->updateAgg(0);  // reset accumulated add val
                        break;
                    }
                    case SDT_DOUBLE: {
                        TypedPredicate<double>* p = \
                                dynamic_cast<TypedPredicate<double>*>(pb);
                        double agg_val = p->Val();
                        flexbldr->Add(agg_val);
                        p->updateAgg(0);  // reset accumulated add val
                        break;
                    }
                    default:  assert(UnsupportedAggDataType==0);
                }
            }
        });

        flexbldr->Finish();

        auto row_data = flatbldr->CreateVector(flexbldr->GetBuffer());
        delete flexbldr;

        // assume no nullbits in the agg results. ?
        nullbits_vector nb(2,0);
        auto nullbits = flatbldr->CreateVector(nb);
        int RID = -1;  // agg recs only, since these are derived data
        flatbuffers::Offset<Tables::Record> row_off = \
            Tables::CreateRecord(*flatbldr, RID, nullbits, row_data);

        // Continue building the ROOT flatbuf's dead vector and rowOffsets vec
        dead_rows.push_back(0);
        offs.push_back(row_off);
        errmsg.append("ERROR processSkyFb() : encode_aggs");
    }
    
    
    std::string query_schema_str;
    for (auto it = query_schema.begin(); it != query_schema.end(); ++it) {
        query_schema_str.append(it->toString() + "\n");
    }

    auto return_data_schema = flatbldr->CreateString(query_schema_str);
    auto db_schema_name = flatbldr->CreateString(root.db_schema_name);
    auto table_name = flatbldr->CreateString(root.table_name);
    auto delete_v = flatbldr->CreateVector(dead_rows);
    auto rows_v = flatbldr->CreateVector(offs);

    auto table = CreateTable(
        *flatbldr,
        root.data_format_type,
        root.skyhook_version,
        root.data_structure_version,
        root.data_schema_version,
        return_data_schema,
        db_schema_name,
        table_name,
        delete_v,
        rows_v,
        offs.size());

    // NOTE: the fb may be incomplete/empty, but must finish() else internal
    // fb lib assert finished() fails, hence we must always return a valid fb
    // and catch any ret error code upstream
    flatbldr->Finish(table);

    
    size_t len = _errmsg_len;
    if(errmsg.length()+1 < len)
        len = errmsg.length()+1;
    strncpy(_errmsg, errmsg.c_str(), len);

    return errcode;
}
int foo3(wasm_exec_env_t exec_env, 
          uint32_t msg_offset, 
          uint32_t buffer_offset, 
          int32_t buf_len,
          int32_t _flatbldr_len,
          uint32_t msg_offset2, 
          uint32_t buffer_offset2, 
          int32_t   buf_len2,
          int32_t _data_schema_len,
          uint32_t msg_offset3, 
          uint32_t buffer_offset3, 
          int32_t buf_len3,
          int32_t msg_length3,
          uint32_t msg_offset4, 
          uint32_t buffer_offset4, 
          int32_t buf_len4,
          int32_t msg_length4,
          uint32_t msg_offset5, 
          uint32_t buffer_offset5, 
          int32_t buf_len5,
          int32_t msg_length5,
          uint32_t msg_offset6, 
          uint32_t buffer_offset6, 
          int32_t buf_len6,
          int32_t msg_length6,
          uint32_t msg_offset7, 
          uint32_t buffer_offset7, 
          int32_t buf_len7,
          int32_t msg_length7 )
{
    wasm_module_inst_t module_inst = get_module_inst(exec_env);
    char *bldrptr;
    char * msg ;

    // do boundary check
    if (!wasm_runtime_validate_app_str_addr(module_inst, msg_offset))
        return 1;
    
    if (!wasm_runtime_validate_app_addr(module_inst,buffer_offset, buf_len))
        return 1;
    // do address conversion
    bldrptr = (char *)wasm_runtime_addr_app_to_native(module_inst, buffer_offset);
    msg = (char *) wasm_runtime_addr_app_to_native(module_inst, msg_offset);

    strncpy(bldrptr, msg, buf_len);
    

    char *ds;
    char * msg2 ;
    // do boundary check
    if (!wasm_runtime_validate_app_str_addr(module_inst, msg_offset2))
        return 1;
    
    if (!wasm_runtime_validate_app_addr(module_inst,buffer_offset2, buf_len2))
        return 1;
    // do address conversion
    ds = (char *)wasm_runtime_addr_app_to_native(module_inst, buffer_offset2);
    msg2 = (char *) wasm_runtime_addr_app_to_native(module_inst, msg_offset2);

    strncpy(ds, msg2, buf_len2);
   
    char *qs;
    char * msg3 ;

    // do boundary check
    if (!wasm_runtime_validate_app_str_addr(module_inst, msg_offset3))
        return 1;
    
    if (!wasm_runtime_validate_app_addr(module_inst,buffer_offset3, buf_len3))
        return 1;
    // do address conversion
    qs = (char *)wasm_runtime_addr_app_to_native(module_inst, buffer_offset3);
    msg3 = (char *) wasm_runtime_addr_app_to_native(module_inst, msg_offset3);

    strncpy(qs, msg3, buf_len3);

    char *qp;
    char * msg4 ;

    // do boundary check
    if (!wasm_runtime_validate_app_str_addr(module_inst, msg_offset4))
        return 1;
    
    if (!wasm_runtime_validate_app_addr(module_inst,buffer_offset4, buf_len4))
        return 1;
    // do address conversion
    qp = (char *)wasm_runtime_addr_app_to_native(module_inst, buffer_offset4);
    msg4 = (char *) wasm_runtime_addr_app_to_native(module_inst, msg_offset4);

    strncpy(qp, msg4, buf_len4);

    char *meta_blob;
    char * msg5 ;

    // do boundary check
    if (!wasm_runtime_validate_app_str_addr(module_inst, msg_offset5))
        return 1;
    
    if (!wasm_runtime_validate_app_addr(module_inst,buffer_offset5, buf_len5))
        return 1;
    // do address conversion
    meta_blob = (char *)wasm_runtime_addr_app_to_native(module_inst, buffer_offset5);
    msg5 = (char *) wasm_runtime_addr_app_to_native(module_inst, msg_offset5);

    strncpy(meta_blob, msg, buf_len);

    char *errmsgptr;
    char * msg6 ;

    // do boundary check
    if (!wasm_runtime_validate_app_str_addr(module_inst, msg_offset6))
        return 1;
    
    if (!wasm_runtime_validate_app_addr(module_inst,buffer_offset6, buf_len6))
        return 1;
    // do address conversion
    errmsgptr = (char *)wasm_runtime_addr_app_to_native(module_inst, buffer_offset6);
    msg6 = (char *) wasm_runtime_addr_app_to_native(module_inst, msg_offset6);

    strncpy(errmsgptr, msg6, buf_len6);
  

    int *rownumptr;
    int * msg7 ;

    // do boundary check
    if (!wasm_runtime_validate_app_str_addr(module_inst, msg_offset7))
        return 1;
    
    if (!wasm_runtime_validate_app_addr(module_inst,buffer_offset7, buf_len7))
        return 1;
    // do address conversion
    rownumptr = (int *)wasm_runtime_addr_app_to_native(module_inst, buffer_offset7);
    msg7 = (int *) wasm_runtime_addr_app_to_native(module_inst, msg_offset7);

    *rownumptr = *msg7;
   
    int ret;

    ret = processSkyFbWASM2(
                                    bldrptr,
                                    _flatbldr_len, 
                                    ds,
                                    _data_schema_len,
                                    qs,
                                    msg_length3,
                                    qp,
                                    msg_length4,
                                    meta_blob,
                                    msg_length5,
                                    errmsgptr,
                                    msg_length6,
                                    rownumptr,
                                    msg_length7);
    
    return ret;

}

void foo4(wasm_exec_env_t exec_env, 
          uint32_t msg_offset, 
          uint32_t buffer_offset, 
          int32_t buf_len,
          int32_t msg_length1)
          {
               wasm_module_inst_t module_inst = get_module_inst(exec_env);
                int *buffer;
                int * msg ;

                // do boundary check
                if (!wasm_runtime_validate_app_str_addr(module_inst, msg_offset))
                    return;
                
                if (!wasm_runtime_validate_app_addr(module_inst,buffer_offset, buf_len))
                    return;
                // do address conversion
                buffer = (int*) wasm_runtime_addr_app_to_native(module_inst, buffer_offset);
                msg = (int*) wasm_runtime_addr_app_to_native(module_inst, msg_offset);

                *buffer = * msg;
                CLS_LOG(20,"In foo4 function, buffer = %d and length = %d",buffer,buf_len);
                CLS_LOG(20,"In foo4 function, length = %d",msg_length1);

          }

}

// Get fb_seq_num from xattr, if not present set to min val
static
int get_fb_seq_num(cls_method_context_t hctx, unsigned int& fb_seq_num) {

    bufferlist fb_bl;
    int ret = cls_cxx_getxattr(hctx, "fb_seq_num", &fb_bl);
    if (ret == -ENOENT || ret == -ENODATA) {
        fb_seq_num = Tables::DATASTRUCT_SEQ_NUM_MIN;
        // If fb_seq_num is not present then insert it in xattr.
    }
    else if (ret < 0) {
        return ret;
    }
    else {
        try {
            bufferlist::iterator it = fb_bl.begin();
            ::decode(fb_seq_num,it);
        } catch (const buffer::error &err) {
            CLS_ERR("ERROR: cls_tabular:get_fb_seq_num: decoding fb_seq_num");
            return -EINVAL;
        }
    }
    return 0;

}

// Insert fb_seq_num to xattr
static
int set_fb_seq_num(cls_method_context_t hctx, unsigned int fb_seq_num) {

    bufferlist fb_bl;
    ::encode(fb_seq_num, fb_bl);
    int ret = cls_cxx_setxattr(hctx, "fb_seq_num", &fb_bl);
    if (ret < 0) {
        return ret;
    }
    return 0;
}

/*
 * Build a skyhook index, insert to omap.
 * Index types are
 * 1. fb_index: points (physically within the object) to the fb
 *    <string fb_num, struct idx_fb_entry>
 *    where fb_num is a sequence number of flatbufs within an obj
 *
 * 2. rec_index: points (logically within the fb) to the relevant row
 *    <string rec-val, struct idx_rec_entry>
 *    where rec-val is the col data value(s) or RID
 *
 */
static
int exec_build_sky_index_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    // iterate over all fbs within an obj and create 2 indexes:
    // 1. for each fb, create idx_fb_entry (physical fb offset)
    // 2. for each row of an fb, create idx_rec_entry (logical row offset)

    // a ceph property encoding the len of each bl in front of the bl,
    // seems to be an int32 currently.
    const int ceph_bl_encoding_len = sizeof(int32_t);

    // fb_seq_num is stored in xattrs and used as a stable counter of the
    // current number of fbs in the object.
    unsigned int fb_seq_num = Tables::DATASTRUCT_SEQ_NUM_MIN;
    int ret = get_fb_seq_num(hctx, fb_seq_num);
    if (ret < 0) {
        CLS_ERR("ERROR: exec_build_sky_index_op: fb_seq_num entry from xattr %d", ret);
        return ret;
    }

    std::string key_fb_prefix;
    std::string key_data_prefix;
    std::string key_data;
    std::string key;
    std::map<std::string, bufferlist> fbs_index;
    std::map<std::string, bufferlist> recs_index;
    std::map<std::string, bufferlist> rids_index;
    std::map<std::string, bufferlist> txt_index;

    // extract the index op instructions from the input bl
    idx_op op;
    try {
        bufferlist::iterator it = in->begin();
        ::decode(op, it);
    } catch (const buffer::error &err) {
        CLS_ERR("ERROR: exec_build_sky_index_op decoding idx_op");
        return -EINVAL;
    }
    Tables::schema_vec idx_schema = Tables::schemaFromString(op.idx_schema_str);

    // obj contains one bl that itself wraps a seq of encoded bls of skyhook fb
    bufferlist wrapped_bls;
    ret = cls_cxx_read(hctx, 0, 0, &wrapped_bls);
    if (ret < 0) {
        CLS_ERR("ERROR: exec_build_sky_index_op: reading obj. %d", ret);
        return ret;
    }

    // decode and process each wrapped bl (each bl contains 1 flatbuf)
    uint64_t off = 0;
    ceph::bufferlist::iterator it = wrapped_bls.begin();
    uint64_t obj_len = it.get_remaining();
    while (it.get_remaining() > 0) {
        off = obj_len - it.get_remaining();
        ceph::bufferlist bl;
        try {
            ::decode(bl, it);  // unpack the next bl
        } catch (ceph::buffer::error&) {
            assert(Tables::BuildSkyIndexDecodeBlsErr==0);
        }

        const char* fb = bl.c_str();   // get fb as contiguous bytes
        int fb_len = bl.length();
        Tables::sky_root root = Tables::getSkyRoot(fb, fb_len);

        // DATA LOCATION INDEX (PHYSICAL data reference):

        // IDX_FB get the key prefix and key_data (fb sequence num)
        ++fb_seq_num;
        key_fb_prefix = buildKeyPrefix(Tables::SIT_IDX_FB, root.db_schema_name,
                                       root.table_name);
        std::string str_seq_num = Tables::u64tostr(fb_seq_num); // key data
        int len = str_seq_num.length();

        // create string of len min chars per type, int32 here
        int pos = len - 10;
        key_data = str_seq_num.substr(pos, len);

        // IDX_FB create the entry struct, encode into bufferlist
        bufferlist fb_bl;
        struct idx_fb_entry fb_ent(off, fb_len + ceph_bl_encoding_len);
        ::encode(fb_ent, fb_bl);
        key = key_fb_prefix + key_data;
        fbs_index[key] = fb_bl;

        // DATA CONTENT INDEXES (LOGICAL data reference):
        // Build the key prefixes for each index type (IDX_RID/IDX_REC/IDX_TXT)
        if (op.idx_type == Tables::SIT_IDX_RID) {
            std::vector<std::string> index_cols;
            index_cols.push_back(Tables::RID_INDEX);
            key_data_prefix = buildKeyPrefix(Tables::SIT_IDX_RID,
                                             root.db_schema_name,
                                             root.table_name,
                                             index_cols);
        }
        if (op.idx_type == Tables::SIT_IDX_REC or
            op.idx_type == Tables::SIT_IDX_TXT) {

            std::vector<std::string> keycols;
            for (auto it = idx_schema.begin(); it != idx_schema.end(); ++it) {
                keycols.push_back(it->name);
            }
            key_data_prefix = Tables::buildKeyPrefix(op.idx_type,
                                                     root.db_schema_name,
                                                     root.table_name,
                                                     keycols);
        }

        // IDX_REC/IDX_RID/IDX_TXT: create the key data for each row
        for (uint32_t i = 0; i < root.nrows; i++) {

            Tables::sky_rec rec = Tables::getSkyRec(static_cast<Tables::row_offs>(root.data_vec)->Get(i));

            switch (op.idx_type) {

                case Tables::SIT_IDX_RID: {

                    // key_data is just the RID val
                    key_data = Tables::u64tostr(rec.RID);

                    // create the entry, encode into bufferlist, update map
                    bufferlist rec_bl;
                    struct idx_rec_entry rec_ent(fb_seq_num, i, rec.RID);
                    ::encode(rec_ent, rec_bl);
                    key = key_data_prefix + key_data;
                    recs_index[key] = rec_bl;
                    break;
                }
                case Tables::SIT_IDX_REC: {

                    // key data is built up from the relevant col vals
                    key_data.clear();

                    auto row = rec.data.AsVector();
                    for (unsigned i = 0; i < idx_schema.size(); i++) {
                        if (i > 0) key_data += Tables::IDX_KEY_DELIM_INNER;
                        key_data += Tables::buildKeyData(
                                            idx_schema[i].type,
                                            row[idx_schema[i].idx].AsUInt64());
                    }

                    // to enforce uniqueness, append RID to key data
                    if (!op.idx_unique) {
                        key_data += (Tables::IDX_KEY_DELIM_OUTER +
                                     Tables::IDX_KEY_DELIM_UNIQUE +
                                     Tables::IDX_KEY_DELIM_INNER +
                                     std::to_string(rec.RID));
                    }

                    // create the entry, encode into bufferlist, update map
                    bufferlist rec_bl;
                    struct idx_rec_entry rec_ent(fb_seq_num, i, rec.RID);
                    ::encode(rec_ent, rec_bl);
                    key = key_data_prefix + key_data;
                    recs_index[key] = rec_bl;
                    break;
                }
                case Tables::SIT_IDX_TXT: {

                    // add each word in the row to a words vector, store as
                    // lower case and and preserve word sequence order.
                    std::vector<std::pair<std::string, int>> words;
                    std::string text_delims;
                    if (!op.idx_text_delims.empty())
                        text_delims = op.idx_text_delims;
                    else
                        text_delims = " \t\r\f\v\n"; // whitespace chars
                    auto row = rec.data.AsVector();
                    for (unsigned i = 0; i < idx_schema.size(); i++) {
                        if (i > 0) key_data += Tables::IDX_KEY_DELIM_INNER;
                        std::string line = \
                            row[idx_schema[i].idx].AsString().str();
                        boost::trim(line);
                        if (line.empty())
                            continue;
                        vector<std::string> elems;
                        boost::split(elems, line, boost::is_any_of(text_delims),
                                            boost::token_compress_on);
                        for (uint32_t i = 0; i < elems.size(); i++) {
                            std::string word = \
                                    boost::algorithm::to_lower_copy(elems[i]);
                            boost::trim(word);

                            // skip stopwords?
                            if (op.idx_ignore_stopwords and
                                Tables::IDX_STOPWORDS.count(word) > 0) {
                                    continue;

                            }
                            words.push_back(std::make_pair(elems[i], i));
                        }
                    }
                    // now create a key and val (an entry struct) for each
                    // word extracted from line
                    for (auto it = words.begin(); it != words.end(); ++it) {

                        key_data.clear();
                        std::string word = it->first;
                        key_data += word;

                        // add the RID for uniqueness,
                        // in case of repeated words within all rows
                        key_data += (Tables::IDX_KEY_DELIM_OUTER +
                                     Tables::IDX_KEY_DELIM_UNIQUE +
                                     Tables::IDX_KEY_DELIM_INNER +
                                     std::to_string(rec.RID));

                        // add the word pos for uniqueness,
                        // in case of repeated words within same row
                        int word_pos = it->second;
                        key_data += (Tables::IDX_KEY_DELIM_INNER +
                                     std::to_string(word_pos));

                        // create the entry, encode into bufferlist, update map
                        bufferlist txt_bl;
                        struct idx_txt_entry txt_ent(fb_seq_num, i,
                                                     rec.RID, word_pos);
                        ::encode(txt_ent, txt_bl);
                        key = key_data_prefix + key_data;
                        txt_index[key] = txt_bl;
                        /*CLS_LOG(20,"kv=%s",
                                 (key+";"+txt_ent.toString()).c_str());*/
                    }
                    break;
                }
                default: {
                    CLS_ERR("exec_build_sky_index_op: %s", (
                            "Index type unknown. type=" +
                            std::to_string(op.idx_type)).c_str());
                }
            }

            // IDX_REC/IDX_RID batch insert to omap (minimize IOs)
            if (recs_index.size() > op.idx_batch_size) {
                ret = cls_cxx_map_set_vals(hctx, &recs_index);
                if (ret < 0) {
                    CLS_ERR("exec_build_sky_index_op: error setting recs index entries %d", ret);
                    return ret;
                }
                recs_index.clear();
            }

            // IDX_TXT batch insert to omap (minimize IOs)
            if (txt_index.size() > op.idx_batch_size) {
                ret = cls_cxx_map_set_vals(hctx, &txt_index);
                if (ret < 0) {
                    CLS_ERR("exec_build_sky_index_op: error setting recs index entries %d", ret);
                    return ret;
                }
                txt_index.clear();
            }
        }  // end foreach row

        // IDX_FB batch insert to omap (minimize IOs)
        if (fbs_index.size() > op.idx_batch_size) {
            ret = cls_cxx_map_set_vals(hctx, &fbs_index);
            if (ret < 0) {
                CLS_ERR("exec_build_sky_index_op: error setting fbs index entries %d", ret);
                return ret;
            }
            fbs_index.clear();
        }
    }  // end while decode wrapped_bls


    // IDX_TXT insert remaining entries to omap
    if (txt_index.size() > 0) {
        ret = cls_cxx_map_set_vals(hctx, &txt_index);
        if (ret < 0) {
            CLS_ERR("exec_build_sky_index_op: error setting recs index entries %d", ret);
            return ret;
        }
    }

    // IDX_REC/IDX_RID insert remaining entries to omap
    if (recs_index.size() > 0) {
        ret = cls_cxx_map_set_vals(hctx, &recs_index);
        if (ret < 0) {
            CLS_ERR("exec_build_sky_index_op: error setting recs index entries %d", ret);
            return ret;
        }
    }
    // IDX_FB insert remaining entries to omap
    if (fbs_index.size() > 0) {
        ret = cls_cxx_map_set_vals(hctx, &fbs_index);
        if (ret < 0) {
            CLS_ERR("exec_build_sky_index_op: error setting fbs index entries %d", ret);
            return ret;
        }
    }

    // Update counter and Insert fb_seq_num to xattr
    ret = set_fb_seq_num(hctx, fb_seq_num);
    if(ret < 0) {
        CLS_ERR("exec_build_sky_index_op: error setting fb_seq_num entry to xattr %d", ret);
        return ret;
    }

    // LASTLY insert a marker key to indicate this index exists,
    // here we are using the key prefix with no data vals
    // TODO: make this a valid entry (not empty_bl), but with empty vals.
    bufferlist empty_bl;
    empty_bl.append("");
    std::map<std::string, bufferlist> index_exists_marker;
    index_exists_marker[key_data_prefix] = empty_bl;
    ret = cls_cxx_map_set_vals(hctx, &index_exists_marker);
    if (ret < 0) {
        CLS_ERR("exec_build_sky_index_op: error setting index_exists_marker %d", ret);
        return ret;
    }

    return 0;
}

/*
 * Build an index from the primary key (orderkey,linenum), insert to omap.
 * Index contains <k=primarykey, v=offset of row within BL>
 */
static
int build_index(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint32_t batch_size;

  try {
    bufferlist::iterator it = in->begin();
    ::decode(batch_size, it);
  } catch (const buffer::error &err) {
    CLS_ERR("ERROR: decoding batch_size");
    return -EINVAL;
  }

  bufferlist bl;
  int ret = cls_cxx_read(hctx, 0, 0, &bl);
  if (ret < 0) {
    CLS_ERR("ERROR: reading obj %d", ret);
    return ret;
  }

  const size_t row_size = 141;
  const char *rows = bl.c_str();
  const size_t num_rows = bl.length() / row_size;

  const size_t order_key_field_offset = 0;
  const size_t line_number_field_offset = 12;


  std::map<string, bufferlist> index;
  // read all rows and extract the key fields
  for (size_t rid = 0; rid < num_rows; rid++) {
    const char *row = rows + rid * row_size;
    const char *o_vptr = row + order_key_field_offset;
    const int order_key_val = *(const int*)o_vptr;
    const char *l_vptr = row + line_number_field_offset;
    const int line_number_val = *(const int*)l_vptr;

    // key
    uint64_t key = ((uint64_t)order_key_val) << 32;
    key |= (uint32_t)line_number_val;
    const std::string strkey = Tables::u64tostr(key);

    // val
    bufferlist row_offset_bl;
    const size_t row_offset = rid * row_size;
    ::encode(row_offset, row_offset_bl);

    if (index.count(strkey) != 0)
      return -EINVAL;

    index[strkey] = row_offset_bl;

    if (index.size() > batch_size) {
      int ret = cls_cxx_map_set_vals(hctx, &index);
      if (ret < 0) {
        CLS_ERR("error setting index entries %d", ret);
        return ret;
      }
      index.clear();
    }
  }

  if (!index.empty()) {
    int ret = cls_cxx_map_set_vals(hctx, &index);
    if (ret < 0) {
      CLS_ERR("error setting index entries %d", ret);
      return ret;
    }
  }

  return 0;
}



// busy loop work
volatile uint64_t __tabular_x;
static void add_extra_row_cost(uint64_t cost)
{
  for (uint64_t i = 0; i < cost; i++) {
    __tabular_x += i;
  }
}

static
int
update_idx_reads(
    cls_method_context_t hctx,
    std::map<int, struct Tables::read_info>& idx_reads,
    bufferlist bl,
    std::string key_fb_prefix,
    std::string key_data_prefix) {

    struct idx_rec_entry rec_ent;
    int ret = 0;
    try {
        bufferlist::iterator it = bl.begin();
        ::decode(rec_ent, it);
    } catch (const buffer::error &err) {
        CLS_ERR("ERROR: decoding query idx_rec_ent");
        return -EINVAL;
    }

    // keep track of the specified row num for this record
    std::vector<unsigned int> row_nums;
    row_nums.push_back(rec_ent.row_num);

    // now build the key to lookup the corresponding flatbuf entry
    std::string key_data = Tables::buildKeyData(Tables::SDT_INT32, rec_ent.fb_num);
    std::string key = key_fb_prefix + key_data;
    struct idx_fb_entry fb_ent;
    bufferlist bl1;
    ret = cls_cxx_map_get_val(hctx, key, &bl1);

    if (ret < 0) {
        if (ret == -ENOENT) {
            CLS_LOG(20,"WARN: NO FB key ENTRY FOUND!! ret=%d", ret);
        }
        else {
            CLS_ERR("cant read map val index for idx_fb_key, %d", ret);
            return ret;
        }
    }

    if (ret >= 0) {
        try {
            bufferlist::iterator it = bl1.begin();
            ::decode(fb_ent, it);
        } catch (const buffer::error &err) {
            CLS_ERR("ERROR: decoding query idx_fb_ent");
            return -EINVAL;
        }

        // our reads are indexed by fb_num
        // either add these row nums to the existing read_info
        // struct for the given fb_num, or create a new one
        auto it = idx_reads.find(rec_ent.fb_num);
        if (it != idx_reads.end()) {
            it->second.rnums.insert(it->second.rnums.end(),
                                    row_nums.begin(),
                                    row_nums.end());
        }
        else {
            idx_reads[rec_ent.fb_num] = \
            Tables::read_info(rec_ent.fb_num,
                              fb_ent.off,
                              fb_ent.len,
                              row_nums);
        }
    }
    return 0;
}

/*
 * Lookup matching records in omap, based on the index specified and the
 * index predicates.  Set the idx_reads info vector with the corresponding
 * flatbuf off/len and row numbers for each matching record.
 */
static
int
read_fbs_index(
    cls_method_context_t hctx,
    std::string key_fb_prefix,
    std::map<int, struct Tables::read_info>& reads)
{

    using namespace Tables;
    int ret = 0;

    unsigned int seq_min = Tables::DATASTRUCT_SEQ_NUM_MIN;
    unsigned int seq_max = Tables::DATASTRUCT_SEQ_NUM_MIN;

    // get the actual max fb seq number
    ret = get_fb_seq_num(hctx, seq_max);
    if (ret < 0) {
        CLS_ERR("error getting fb_seq_num entry from xattr %d", ret);
        return ret;
    }

    // fb seq num grow monotically, so try to read each key
    for (unsigned int i = seq_min; i <= seq_max; i++) {

        // create key for this seq num.
        std::string key_data = Tables::buildKeyData(Tables::SDT_INT32, i);
        std::string key = key_fb_prefix + key_data;

        bufferlist bl;
        ret = cls_cxx_map_get_val(hctx, key, &bl);

        // a seq_num may not be present due to fb deleted/compaction
        // if key not found, just continue (this is not an error)
        if (ret < 0) {
            if (ret == -ENOENT) {
                // CLS_LOG(20, "omap entry NOT found for key=%s", key.c_str());
                continue;
            }
            else {
                CLS_ERR("Cannot read omap entry for key=%s", key.c_str());
                return ret;
            }
        }

        // key found so decode the entry and set the read vals.
        if (ret >= 0) {
            // CLS_LOG(20, "omap entry found for key=%s", key.c_str());
            struct idx_fb_entry fb_ent;
            try {
                bufferlist::iterator it = bl.begin();
                ::decode(fb_ent, it);
            } catch (const buffer::error &err) {
                CLS_ERR("ERROR: decoding idx_fb_ent for key=%s", key.c_str());
                return -EINVAL;
            }
            reads[i] = Tables::read_info(i, fb_ent.off, fb_ent.len, {});
        }
    }
    return 0;
}




/*
    Check for index existence, always used before trying to perform index reads
    We check omap for the presence of the base key (prefix only), which is used
    to indicate the index exists.
    Note that even if key does exists, key_val_map will be empty since there is
    no associated record data for this key.
*/
static
bool
sky_index_exists (cls_method_context_t hctx, std::string key_prefix)
{
    std::map<std::string, bufferlist> key_val_map;
    bufferlist dummy_bl;
    int ret = cls_cxx_map_get_val(hctx, key_prefix, &dummy_bl);
    if (ret < 0 && ret != -ENOENT) {
        CLS_ERR("Cannot read idx_rec entry for key, errorcode=%d", ret);
        return false;
    }

    // If no entries were found for this key, assume index does not exist.
    if (ret == -ENOENT)
        return false;

    return true;
}


/*
    Decide to use index or not.
    Check statistics and index predicates, if expected selectivity is high
    enough then use the index (return true) else if low selectivity too many
    index entries will match and we should not use the index (return false).
    Returning false indicates to use a table scan instead of index.
*/
static
bool
use_sky_index(
        cls_method_context_t hctx,
        std::string index_prefix,
        Tables::predicate_vec index_preds)
{
    // we assume to use by default, since the planner requested it.
    bool use_index = true;

    bufferlist bl;
    int ret = cls_cxx_map_get_val(hctx, index_prefix, &bl);
    if (ret < 0 && ret != -ENOENT) {
        CLS_ERR("Cannot read idx_rec entry for key, errorcode=%d", ret);
        return false;
    }

    // If an entry was found for this index key, check the statistics for
    // each predicate to see if it is expected to be highly selective.
    if (ret != -ENOENT) {

        // TODO: this should be based on a cost model, not a fixed value.
        const float SELECTIVITY_HIGH_VAL = 0.10;

        // for each pred, check the bl idx_stats struct and decide selectivity
        for (auto it = index_preds.begin(); it != index_preds.end(); ++it) {

            // TODO: compute this from the predicate value and stats struct
            float expected_selectivity = 0.10;
            if (expected_selectivity <= SELECTIVITY_HIGH_VAL)
                use_index &= true;
            else
                use_index &= false;
        }
    }
    return use_index;
}

/*
 * Lookup matching records in omap, based on the index specified and the
 * index predicates.  Set the idx_reads info vector with the corresponding
 * flatbuf off/len and row numbers for each matching record.
 */

static
int
read_sky_index(
    cls_method_context_t hctx,
    Tables::predicate_vec index_preds,
    std::string key_fb_prefix,
    std::string key_data_prefix,
    int index_type,
    int idx_batch_size,
    std::map<int, struct Tables::read_info>& idx_reads) {

    using namespace Tables;
    int ret = 0, ret2 = 0;
    std::vector<std::string> keys;   // to contain all keys found after lookups

    // for each fb_seq_num, a corresponding read_info struct to
    // indicate the relevant rows within a given fb.
    // fb_seq_num is used as key, so that subsequent reads will always be from
    // a higher byte offset, if that matters.

    // build up the key data portion from the idx pred vals.
    // assumes all indexes here are integers, we extract the predicate vals
    // as ints and use our uint to padded string method to build the keys
    std::string key_data;
    for (unsigned i = 0; i < index_preds.size(); i++) {
        uint64_t val = 0;

        switch (index_preds[i]->colType()) {
            case SDT_INT8:
            case SDT_INT16:
            case SDT_INT32:
            case SDT_INT64: {  // TODO: support signed ints in index ranges
                int64_t v = 0;
                extract_typedpred_val(index_preds[i], v);
                val = static_cast<uint64_t>(v);  // force to unsigned for now.
                break;
            }
            case SDT_UINT8:
            case SDT_UINT16:
            case SDT_UINT32:
            case SDT_UINT64: {
                extract_typedpred_val(index_preds[i], val);
                break;
            }
            default:
                assert (BuildSkyIndexUnsupportedColType==0);
        }

        if (i > 0)  // add delim for multicol index vals
            key_data += IDX_KEY_DELIM_INNER;
        key_data += buildKeyData(index_preds[i]->colType(), val);
    }
    std::string key = key_data_prefix + key_data;

    // Add base key when all index predicates include equality
    if (check_predicate_ops_all_include_equality(index_preds)) {
        keys.push_back(key);
    }

    // Find the starting key for range query keys:
    // 1. Greater than predicates we start after the base key,
    // 2. Less than predicates we start after the prefix of the base key.
    std::string start_after = "";
    if (check_predicate_ops(index_preds, SOT_geq) or
        check_predicate_ops(index_preds, SOT_gt)) {

        start_after = key;
    }
    else if (check_predicate_ops(index_preds, SOT_lt) or
             check_predicate_ops(index_preds, SOT_leq)) {

        start_after = key_data_prefix;
    }

    // Get keys in batches at a time and print row number/ offset detail
    // Equality query does not loop  TODO: this is not true for non-unique idx!
    bool stop = false;
    if (start_after.empty()) stop = true;

    // Retrieve keys for range queries in batches of "max_to_get"
    // until no more keys
    bool more = true;
    int max_to_get = idx_batch_size;
    std::map<std::string, bufferlist> key_val_map;
    while(!stop) {
        ret2 = cls_cxx_map_get_vals(hctx, start_after, string(),
                                    max_to_get, &key_val_map, &more);

        if (ret2 < 0 && ret2 != -ENOENT) {
            CLS_ERR("cant read map val index rec for idx_rec key %d", ret2);
            return ret2;
        }

        // If no more entries found break out of the loop
        if (ret2 == -ENOENT || key_val_map.size() == 0) {
            break;
        }

        if (ret2 >= 0) {
            try {
                for (auto it = key_val_map.cbegin();
                          it != key_val_map.cend(); it++) {
                    const std::string& key1 = it->first;
                    bufferlist record_bl_entry = it->second;

                    // Break if keyprefix in fetched key does not match that
                    // passed by user, means we have gone too far, possibly
                    // into keys for another index.
                    if (key1.find(key_data_prefix) == std::string::npos) {
                        stop = true;
                        break;
                    }

                    // If this is the last key, update start_after value
                    if (std::next(it, 1) == key_val_map.cend()) {
                        start_after = key1;
                    }

                    // break if key matches or exceeds key passed by the user
                    // prevents going into the next index (next key prefix)
                    if (check_predicate_ops(index_preds, SOT_lt) or
                        check_predicate_ops(index_preds, SOT_leq)) {
                        //~ if(key_val_map.find(key) != key_val_map.end()) {
                            //~ stop = true;
                            //~ break;
                        //~ }
                        if (key1 == key) {   /// TODO: is this  extra check needed?
                            stop = true;
                            break;
                        }
                        else if (key1 > key) {  // Special handling for leq
                            if (check_predicate_ops(index_preds, SOT_leq) and
                                compare_keys(key, key1)) {
                                stop = false;
                            }
                            else {
                                stop = true;
                                break;
                            }
                        }
                    }

                    // Skip equality entries in geq query
                    if (check_predicate_ops(index_preds, SOT_gt) and
                        compare_keys(key, key1)) {
                        continue;
                    }

                    // Set the idx_reads info vector with the corresponding
                    // flatbuf off/len and row numbers for each matching record
                    ret2 = update_idx_reads(hctx, idx_reads, record_bl_entry,
                                            key_fb_prefix, key_data_prefix);
                    if(ret2 < 0)
                        return ret2;
                }
            } catch (const buffer::error &err) {
                    CLS_ERR("ERROR: decoding query idx_rec_ent");
                    return -EINVAL;
            }
        }
    }


    // lookup key in omap to get the row offset
    if (!keys.empty()) {
        for (unsigned i = 0; i < keys.size(); i++) {
            bufferlist record_bl_entry;
            ret = cls_cxx_map_get_val(hctx, keys[i], &record_bl_entry);
            if (ret < 0 && ret != -ENOENT) {
                CLS_ERR("cant read map val index rec for idx_rec key %d", ret);
                return ret;
            }
            if (ret >= 0) {
                ret2 = update_idx_reads(hctx,
                                        idx_reads,
                                        record_bl_entry,
                                        key_fb_prefix,
                                        key_data_prefix);
                if (ret2 < 0)
                    return ret2;
            } else  {
                // no rec found for key
            }
        }
    }
    return 0;
}

/*
 * Primary method to process queries
 */
static
int exec_query_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    int ret = 0;
    uint64_t rows_processed = 0;
    uint64_t read_ns = 0;
    uint64_t eval_ns = 0;
    bufferlist result_bl;  // result set to be returned to client.
    query_op op;

    // extract the query op to get the query request params
    try {
        bufferlist::iterator it = in->begin();
        ::decode(op, it);
    } catch (const buffer::error &err) {
        CLS_ERR("ERROR: decoding query op");
        return -EINVAL;
    }
    std::string msg = op.toString();
    std::replace(msg.begin(), msg.end(), '\n', ' ');

    if (op.query == "flatbuf") {

        using namespace Tables;

        // hold result of index lookups or read all flatbufs
        bool index1_exists = false;
        bool index2_exists = false;
        bool use_index1 = false;
        bool use_index2 = false;
        std::map<int, struct read_info> reads;
        std::map<int, struct read_info> idx1_reads;
        std::map<int, struct read_info> idx2_reads;

        // fastpath means we skip processing rows and just return all rows,
        // i.e., the entire obj
        // NOTE: fastpath will not increment rows_processed since we do nothing
        if (op.fastpath == true) {
            bufferlist b;  // to hold the obj data.
            uint64_t start = getns();
            ret = cls_cxx_read(hctx, 0, 0, &b);  // read entire object.
            if (ret < 0) {
              CLS_ERR("ERROR: reading flatbuf obj %d", ret);
              return ret;
            }
            read_ns = getns() - start;
            result_bl = b;

        } else {

            // data_schema is the table's current schema
            // TODO: redundant, this is also stored in the fb, extract from fb?
            schema_vec data_schema = schemaFromString(op.data_schema);
            
            // query_schema is the query schema
            schema_vec query_schema = schemaFromString(op.query_schema);

            // predicates to be applied, if any
            predicate_vec query_preds = predsFromString(data_schema,
                                                        op.query_preds);

            std::string ds = schemaToString(data_schema);
            std::string qs = schemaToString(query_schema);
            std::string qp = predsToString(query_preds, data_schema);

            // required for index plan or scan plan if index plan not chosen.
            predicate_vec index_preds;
            predicate_vec index2_preds;

            std::string key_fb_prefix = buildKeyPrefix(SIT_IDX_FB,
                                                       op.db_schema_name,
                                                       op.table_name);
            // lookup correct flatbuf and potentially set specific row nums
            // to be processed next in processFb()
            if (op.index_read) {

                // get info for index1
                schema_vec index_schema = \
                        schemaFromString(op.index_schema);

                index_preds = \
                        predsFromString(data_schema, op.index_preds);

                std::vector<std::string> index_cols = \
                        colnamesFromSchema(index_schema);

                std::string key_data_prefix = \
                        buildKeyPrefix(op.index_type,
                                       op.db_schema_name,
                                       op.table_name,
                                       index_cols);

                // get info for index2
                schema_vec index2_schema = \
                        schemaFromString(op.index2_schema);
                index2_preds = \
                        predsFromString(data_schema, op.index2_preds);

                std::vector<std::string> index2_cols = \
                        colnamesFromSchema(index2_schema);

                std::string key2_data_prefix = \
                        buildKeyPrefix(op.index2_type,
                                       op.db_schema_name,
                                       op.table_name,
                                       index2_cols);

                // verify if index1 is present in omap
                index1_exists = sky_index_exists(hctx,
                                                 key_data_prefix);

                // check local statistics, decide to use or not.
                if (index1_exists)
                    use_index1 = use_sky_index(hctx,
                                               key_data_prefix,
                                               index_preds);

                if (use_index1) {

                    // check for case of multicol index but not all equality.
                    if (index_cols.size() > 1 and
                        !check_predicate_ops_all_equality(index_preds)) {

                        // NOTE: mutlicol indexes only support range queries
                        // over first col (but all cols for equality queries)
                        // so to preserve correctness, here we (redundantly)
                        // add all of the index preds to the query preds
                        // to preserve correctness but we do not remove the
                        // extra non-equality preds from the index preds
                        // since in the future index queries will support
                        // ranges on multicols.

                        query_preds.reserve(query_preds.size() +
                                            index_preds.size());
                        for (unsigned i = 0; i < index_preds.size(); i++) {
                            query_preds.push_back(index_preds[i]);
                        }
                    }

                    // index lookup to set the read requests, if any rows match
                    ret = read_sky_index(hctx,
                                         index_preds,
                                         key_fb_prefix,
                                         key_data_prefix,
                                         op.index_type,
                                         op.index_batch_size,
                                         idx1_reads);
                    if (ret < 0) {
                        CLS_ERR("ERROR: do_index_lookup failed. %d", ret);
                        return ret;
                    }
                    CLS_LOG(20, "exec_query_op: index1 found %lu entries",
                            idx1_reads.size());

                    reads = idx1_reads;  // populate with reads from index1

                    // check for second index/index plan type and set READs.
                    switch (op.index_plan_type) {

                    case SIP_IDX_STANDARD: {
                        break;
                    }

                    case SIP_IDX_INTERSECTION:
                    case SIP_IDX_UNION: {

                        // verify if index2 is present in omap
                        index2_exists = sky_index_exists(hctx,
                                                         key2_data_prefix);

                        // check local statistics, decide to use or not.
                        if (index2_exists)
                            use_index2 &= use_sky_index(hctx,
                                                        key2_data_prefix,
                                                        index2_preds);

                        if (use_index2) {

                            // check for case of multicol index but not all equality.
                            if (index2_cols.size() > 1 and
                                !check_predicate_ops_all_equality(index2_preds)) {

                                // NOTE: same reasoning as above for index1_preds
                                query_preds.reserve(query_preds.size() +
                                                    index2_preds.size());
                                for (unsigned i = 0; i < index2_preds.size(); i++) {
                                    query_preds.push_back(index2_preds[i]);
                                }
                            }

                            ret = read_sky_index(hctx,
                                                 index2_preds,
                                                 key_fb_prefix,
                                                 key2_data_prefix,
                                                 op.index2_type,
                                                 op.index_batch_size,
                                                 idx2_reads);
                            if (ret < 0) {
                                CLS_ERR("ERROR: do_index2_lookup failed. %d",
                                        ret);
                                return ret;
                            }

                            CLS_LOG(20, "exec_query_op: index2 found %lu entries",
                                    idx2_reads.size());

                            // INDEX PLAN (INTERSECTION or UNION)
                            // for each fbseq_num in idx1 reads, check if idx2
                            // has any rows for same fbseq_num, perform
                            // intersection or union of rows, store resulting
                            // rows into our reads vector.
                            ///reads.clear();

                            // for union always "populate" the first vector
                            // because we always iterate over that one, so it
                            // cannot be empty
                            if (idx1_reads.empty() and
                                op.index_plan_type == SIP_IDX_UNION)   {

                                idx1_reads = idx2_reads;
                            }

                            for (auto it1 = idx1_reads.begin();
                                      it1 != idx1_reads.end(); ++it1) {

                                int fbnum = it1->first;
                                auto it2 = idx2_reads.find(fbnum);

                                // if not found but we need to do union, then
                                // point the second iterator back to the first.
                                if (it2 == idx2_reads.end() and
                                    op.index_plan_type == SIP_IDX_UNION) {

                                    it2 = it1;
                                }

                                if (it2 != idx2_reads.end()) {
                                    struct Tables::read_info ri1 = it1->second;
                                    struct Tables::read_info ri2 = it2->second;
                                    std::vector<unsigned> rnums1 = ri1.rnums;
                                    std::vector<unsigned> rnums2 = ri2.rnums;
                                    std::sort(rnums1.begin(), rnums1.end());
                                    std::sort(rnums2.begin(), rnums2.end());
                                    std::vector<unsigned> result_rnums(
                                            rnums1.size() + rnums2.size());

                                    switch (op.index_plan_type) {

                                    case SIP_IDX_INTERSECTION: {
                                        auto it = std::set_intersection(
                                                        rnums1.begin(),
                                                        rnums1.end(),
                                                        rnums2.begin(),
                                                        rnums2.end(),
                                                        result_rnums.begin());
                                        result_rnums.resize(it -
                                                        result_rnums.begin());
                                        break;
                                    }

                                    case SIP_IDX_UNION: {
                                        auto it = std::set_union(
                                                        rnums1.begin(),
                                                        rnums1.end(),
                                                        rnums2.begin(),
                                                        rnums2.end(),
                                                        result_rnums.begin());
                                        result_rnums.resize(it -
                                                        result_rnums.begin());
                                        break;
                                    }

                                    default: {
                                        // none
                                    }}

                                    if (!result_rnums.empty()) {
                                        reads[fbnum] = Tables::read_info(
                                                            ri1.fb_seq_num,
                                                            ri1.off,
                                                            ri1.len,
                                                            result_rnums);
                                    }
                                }
                            }
                        } // end if (use_index2)
                        break;
                    }

                    default:
                        use_index1 = false;  // no index plan type specified.
                        use_index2 = false;  // no index plan type specified.
                    }
                }  // end if (use_index1)
            }


            /*
             * At this point we either used an index or not, act accordingly:
             *
             * if we did use an index, then either we already
             *      1. found matching entries and set the reads vector with a
             *         sequence of fbs containing the data
             *      or
             *      2. found no matching index entries and so the reads vector
             *         is empty, which is ok
             *
             * if we did NOT use an index, then either we need to either
             *      1. set the reads vector with a single read for the entire
             *         object at once
             *      or
             *      2. set the reads vector with a sequence of fbs if we are
             *         mem constrained.
             */

            if (op.index_read) {
                // If we were requested to do an index plan but locally
                // decided not to use one or both indexes, then we must add
                // those requested index predicates to our query_preds so those
                // predicates can be applied during the data scan operator
                if (!use_index1) {
                    if (!index_preds.empty()) {
                            query_preds.insert(
                                query_preds.end(),
                                std::make_move_iterator(index_preds.begin()),
                                std::make_move_iterator(index_preds.end()));
                    }
                }

                if (!use_index2) {
                    if (!index2_preds.empty()) {
                            query_preds.insert(
                                query_preds.end(),
                                std::make_move_iterator(index2_preds.begin()),
                                std::make_move_iterator(index2_preds.end()));
                    }
                }
            }


            if (!op.index_read or
                (op.index_read and (!use_index1 and !use_index2))) {
                // if no index read was requested,
                // or it was requested and we decided not to use either index,
                // then we must read the entire object (perform table scan).
                //
                // So here we decide to either
                // 1) read it all at once into mem or
                // 2) read each fb into mem in sequence to hopefully conserve
                //    mem during read + processing.

                // default, assume we have plenty of mem avail.
                bool read_full_object = true;

                if (op.mem_constrain) {

                    // try to set the reads[] with the fb sequence
                    int ret = read_fbs_index(hctx, key_fb_prefix, reads);

                    if (reads.empty())
                        CLS_LOG(20,"exec_query_op: WARN: No FBs index entries found.");

                    // if we found the fb sequence of offsets, then we
                    // no longer need to read the full object.
                    if (ret >= 0 and !reads.empty())
                        read_full_object = false;
                }

                // if we must read the full object, we set the reads[] to
                // contain a single read, indicating the entire object.
                if (read_full_object) {
                    int fb_seq_num = Tables::DATASTRUCT_SEQ_NUM_MIN;
                    int off = 0;
                    int len = 0;
                    std::vector<unsigned int> rnums = {};
                    struct read_info ri(fb_seq_num, off, len, {});
                    reads[fb_seq_num] = ri;
                }
            }

            // now we can decode and process each bl in the obj, specified
            // by each read request.
            // NOTE: 1 bl contains exactly 1 flatbuf.
            // weak ordering in map will iterate over fb nums in sequence
            for (auto it = reads.begin(); it != reads.end(); ++it) {
                bufferlist b;
                size_t off = it->second.off;
                size_t len = it->second.len;
                std::vector<unsigned int> row_nums = it->second.rnums;
                std::string msg = "off=" + std::to_string(off) +
                                  ";len=" + std::to_string(len);

                uint64_t start = getns();
                ret = cls_cxx_read(hctx, off, len, &b);
                if (ret < 0) {
                  std::string msg = std::to_string(ret) + "reading obj at off="
                                    + std::to_string(off) + ";len="
                                    + std::to_string(len);
                  CLS_ERR("ERROR: %s", msg.c_str());
                  return ret;
                }
                read_ns += getns() - start;
                start = getns();
                ceph::bufferlist::iterator it2 = b.begin();
                while (it2.get_remaining() > 0) {

                    // unpack the next data stucture (ds) in sequence
                    // obj contains a sequence of fbmeta's, each encoded as bl
                    // object layout: bl1,bl2,bl3,....bln
                    // where bl1=fbmeta
                    // where bl2=fbmeta
                    // ...

                    bufferlist bl;
                    try {
                        ::decode(bl, it2);  // TODO: decode as FB_META
                    } catch (const buffer::error &err) {
                        CLS_ERR("ERROR: decoding data from it2 (ds sequence");
                        return -EINVAL;
                    }

                    /*
                    * NOTE:
                    *
                    * to manually test new formats you can append your new serialized
                    * formatted data as a char* into a bl, then set optional args to
                    * false and specify the format type such as this:
                    * sky_meta meta = getSkyMeta(bl, false, SFT_FLATBUF_FLEX_ROW);
                    *
                    * which creates a new fbmeta from your new type of bl data.
                    * then you can check the fields:
                    * std::cout << "meta.blob_format:" << meta.blob_format << endl;
                    */

                    // default usage here assumes the fbmeta is already in the bl
                    sky_meta meta = getSkyMeta(&bl);
                    CLS_LOG(20, "sky_meta: meta.blob_format=%d", meta.blob_format);
                    CLS_LOG(20, "sky_meta: meta.blob_data=%p", &meta.blob_data[0]);
                    CLS_LOG(20, "sky_meta: meta.blob_size=%lu", meta.blob_size);
                    CLS_LOG(20, "sky_meta: meta.blob_deleted=%d", meta.blob_deleted);
                    CLS_LOG(20, "sky_meta: meta.blob_orig_off=%lu", meta.blob_orig_off);
                    CLS_LOG(20, "sky_meta: meta.blob_orig_len=%lu", meta.blob_orig_len);
                    CLS_LOG(20, "sky_meta: meta.blob_compression=%d", meta.blob_compression);

                    // debug/accounting
                    std::string errmsg;
                    int ds_rows_processed = 0;

                    // CREATE An FB_META, start with an empty builder first
                    flatbuffers::FlatBufferBuilder *meta_builder =      \
                        new flatbuffers::FlatBufferBuilder();

                    // this code block is only used for accounting (rows processed)
                    int root_rows = 0;
                    switch (meta.blob_format) {

                        case SFT_FLATBUF_FLEX_ROW:
                        case SFT_ARROW:
                        case SFT_JSON: {
                            sky_root root = Tables::getSkyRoot(meta.blob_data,
                                                               meta.blob_size,
                                                               meta.blob_format);
                            root_rows += root.nrows;
                            break;
                        }

                        case SFT_FLATBUF_UNION_ROW:
                        case SFT_FLATBUF_UNION_COL: {

                            // extract ptr to meta data blob
                            const char* blob_dataptr = \
                                reinterpret_cast<const char*>(meta.blob_data);
                            size_t blob_sz = meta.blob_size;

                            // extract bl_seq bufferlist
                            ceph::bufferlist bl_seq;
                            bl_seq.append(blob_dataptr, blob_sz);

                            // just need to grab the first bufferlist in bl_seq
                            ceph::bufferlist::iterator it_bl_seq = bl_seq.begin();

                            // decode decrements get_remaining by moving iterator
                            ceph::bufferlist bl;
                            ::decode(bl, it_bl_seq);
                            const char* dataptr = bl.c_str();
                            size_t datasz = bl.length();

                            // get the number of rows returned for accounting
                            sky_root root = getSkyRoot(dataptr,
                                                       datasz,
                                                       SFT_FLATBUF_UNION_ROW);
                            root_rows = root.nrows;
                            break;
                        }
                    } //switch

                    // call associated process method based on ds type
                    switch (meta.blob_format) {

                    case SFT_JSON: {

                        sky_root root = \
                            Tables::getSkyRoot(meta.blob_data,
                                               meta.blob_size,
                                               meta.blob_format);

                        // TODO: write json processing function,
                        // now we just pass thru the original
                        // meta.data_blob as the processed result data
                        char* orig_data = const_cast<char*>(meta.blob_data);
                        size_t orig_size = meta.blob_size;

                        // TODO: call processJSON() here. See below for similar
                        // function required here to get result data
                        char* result_data = orig_data;
                        size_t result_size = orig_size;

                        createFbMeta(meta_builder,
                                     SFT_FLATBUF_FLEX_ROW,
                                     reinterpret_cast<unsigned char*>(
                                        result_data),
                                     result_size);
                        break;
                    }

                    case SFT_FLATBUF_FLEX_ROW: {

                        // int bldr_size = 1024;
                        // flatbuffers::FlatBufferBuilder result_builder(bldr_size);
                        // temporary toggle for wasm execution testing
                        bool wasm = true;

                        if (!wasm) {
                        ret = processSkyFb(result_builder,
                                           data_schema,
                                           query_schema,
                                           query_preds,
                                           meta.blob_data,
                                           meta.blob_size,
                                           errmsg,
                                           row_nums);
                        }
                        else {

                            char *wasm_file = NULL;
                            uint8_t *wasm_file_buf = NULL;
                            int wasm_file_size;
                            wasm_module_t wasm_module = NULL;

                            wasm_module_inst_t wasm_module_inst = NULL;
                            wasm_function_inst_t func;
                            char error_buf[128];
                            const char *exception;
                            uint32_t func_argv[8] = { 0 };
                            wasm_exec_env_t exec_env;
                            uint32_t argv2[20];
                            uint32_t argv3[4]={0};

                            char * buffer = NULL;
                            int32_t buffer_for_wasm;

                            wasm_file = "/usr/local/bin/test.wasm";

                            static NativeSymbol native_symbols[] = 
                            {
                                EXPORT_WASM_API(foo),
                                EXPORT_WASM_API(foo2),
                                EXPORT_WASM_API(foo3)
                            };

                            if (!wasm_runtime_init())
                                return 0;
                        
                            int n_native_symbols = sizeof(native_symbols) / sizeof(NativeSymbol);
                            if (!wasm_runtime_register_natives("env",
                                                        native_symbols, 
                                                        n_native_symbols)) {
                                return 0;
                            }

                            long size;

                            ifstream file ("/usr/local/bin/test.wasm", ios::in|ios::binary|ios::ate);
                            size = file.tellg();
                            file.seekg (0, ios::beg);
                            buffer = new char [size];
                            file.read (buffer, size);
                            file.close();
                            wasm_file_size = (int)size;

                            if (!(wasm_file_buf = (uint8_t*) buffer))
                                return 0;

                            /* load WASM module */
                            if (!(wasm_module = wasm_runtime_load(wasm_file_buf, wasm_file_size,
                                                                error_buf, sizeof(error_buf)))) {
                                CLS_LOG(20,"Runtime load error ", error_buf);
                                return 0;
                            }

                            /* instantiate the module */
                            if (!(wasm_module_inst = wasm_runtime_instantiate(wasm_module,
                                                                            32 * 1024, /* stack size */
                                                                            32 * 1024,  /* heap size */
                                                                            error_buf,
                                                                            sizeof(error_buf)))) {
                                CLS_LOG(20,"Instantiate error",error_buf );
                                return 0;
                            }

                            func = wasm_runtime_lookup_function(wasm_module_inst, "_main", "(i32i32)i32");
                            if (!func) {
                                CLS_LOG(20,"Lookup function _main failed.");
                                return 0;
                            }

                            exec_env = wasm_runtime_create_exec_env(wasm_module_inst, 32 * 1024);

                            wasm_runtime_call_wasm(exec_env, func, 2, func_argv);
                            if ((exception = wasm_runtime_get_exception(wasm_module_inst))) {
                                CLS_LOG(20,"Got exception: ",exception);
                                return 0;
                            }

                            func = wasm_runtime_lookup_function(wasm_module_inst, "_fib", "(i32)i32");
                            if (!func) {
                                CLS_LOG(20,"Lookup function _main failed." );
                                return 0;
                            }
                            exec_env = wasm_runtime_create_exec_env(wasm_module_inst, 32 * 1024);

                            func_argv[0] = 20;
                            wasm_runtime_call_wasm(exec_env, func, 1, func_argv);
                            if ((exception = wasm_runtime_get_exception(wasm_module_inst))) {
                                CLS_LOG(20,"Got exception: ",exception);
                                return 0;
                            }
                            CLS_LOG(20,"Call fib(20), return = %d" , func_argv[0] );

                            func = wasm_runtime_lookup_function(wasm_module_inst, "_passString", "(i32)i32");
                            if (!func) {
                                CLS_LOG(20,"Lookup function _main failed.");
                                return 0;
                            }
                            exec_env = wasm_runtime_create_exec_env(wasm_module_inst, 32 * 1024);

                            buffer_for_wasm = wasm_runtime_module_malloc(wasm_module_inst, 10, (void**)&buffer);
                            if(buffer_for_wasm != 0)
                            {
                                strncpy(buffer, "hello", 10);	// use native address for accessing in runtime
                                //std::cout<<"buffer : "<<buffer<<std::endl;
                                argv2[0] = buffer_for_wasm;  	// pass the buffer address for WASM space.
                                argv2[1] = 10;					// the size of buffer
                                wasm_runtime_call_wasm(exec_env, func, 2, argv2);
                                if ((exception = wasm_runtime_get_exception(wasm_module_inst))) {
                                    CLS_LOG(20,"Got exception:%s",exception);
                                    return 0;
                                }
                            wasm_runtime_module_free(wasm_module_inst, buffer_for_wasm);
                            }

                            func = wasm_runtime_lookup_function(wasm_module_inst, "_sum", "(i32i32)i32");
                            if (!func) {
                                CLS_LOG(20,"Lookup function _sum failed.");
                                return 0;
                            }
                            exec_env = wasm_runtime_create_exec_env(wasm_module_inst, 32 * 1024);

                            func_argv[0] = 2;
                            func_argv[1] = 20;

                            wasm_runtime_call_wasm(exec_env, func, 2, func_argv);
                            if ((exception = wasm_runtime_get_exception(wasm_module_inst))) {
                                CLS_LOG(20,"Got exception:%s ", exception );
                                return 0;
                            }
                            CLS_LOG(20,"Call _sum(2,20), return = %d",func_argv[0]);


                            // convert all params to native char or int types for wasm
                            char* bldrptr = reinterpret_cast<char*>(&result_builder);
                            std::string ds = schemaToString(data_schema);
                            std::string qs = schemaToString(query_schema);
                            std::string qp = predsToString(query_preds, data_schema);
                            int ERRMSG_MAX_LEN = 256;
                            char* errmsg_ptr = (char*) calloc(ERRMSG_MAX_LEN, sizeof(char));
                            int row_nums_size = static_cast<int>(row_nums.size());
                            int* row_nums_ptr = (int*) calloc(row_nums_size, sizeof(int));
                            std::copy(row_nums.begin(), row_nums.end(), row_nums_ptr);


                            meta_blob_data = const_cast<char*>(meta.blob_data);
                            meta_blob_size = meta.blob_size;

                            func = wasm_runtime_lookup_function(wasm_module_inst, "_process", "(i32i32i32i32i32i32i32i32i32i32i32i32i32i32)i32");
                            if (!func) {
                                CLS_LOG(20,"Lookup function _process failed.");
                                return 0;
                            }
                            exec_env = wasm_runtime_create_exec_env(wasm_module_inst, 32 * 1024);

                            char * bldrptr_buffer = NULL;
                            int32_t bldrptr_buffer_for_wasm;
                            char * ds_buffer = NULL;
                            int ds_buffer_for_wasm;
                            char * qs_buffer = NULL;
                            int32_t qs_buffer_for_wasm;
                            char * qp_buffer = NULL;
                            int32_t qp_buffer_for_wasm;
                            char * meta_buffer = NULL;
                            int32_t meta_buffer_for_wasm;
                            char * error_buffer = NULL;
                            int32_t error_buffer_for_wasm;
                            int * row_nums_ptr_buffer = NULL;
                            int32_t row_nums_ptr_buffer_for_wasm;

                            bldrptr_buffer_for_wasm = wasm_runtime_module_malloc(wasm_module_inst, 1000, (void**)&bldrptr_buffer);
                            ds_buffer_for_wasm = wasm_runtime_module_malloc(wasm_module_inst, 1000, (void**)&ds_buffer);
                            qs_buffer_for_wasm = wasm_runtime_module_malloc(wasm_module_inst, 1000, (void**)&qs_buffer);
                            qp_buffer_for_wasm = wasm_runtime_module_malloc(wasm_module_inst, 1000, (void**)&qp_buffer);
                            meta_buffer_for_wasm = wasm_runtime_module_malloc(wasm_module_inst, 8000, (void**)&meta_buffer);
                            error_buffer_for_wasm = wasm_runtime_module_malloc(wasm_module_inst, 1000, (void**)&error_buffer);
                            row_nums_ptr_buffer_for_wasm = wasm_runtime_module_malloc(wasm_module_inst, 1000, (void**)&row_nums_ptr_buffer);

                            if(bldrptr_buffer_for_wasm != 0 && ds_buffer_for_wasm != 0){
                                strncpy(bldrptr_buffer, bldrptr, sizeof(bldrptr));	// use native address for accessing in runtime
                                strcpy(ds_buffer, ds.c_str());	// use native address for accessing in runtime
                                strcpy(qs_buffer, qs.c_str());
                                strcpy(qp_buffer, qp.c_str());
                                strncpy(meta_buffer, meta_blob_data, meta_blob_size);
                                strncpy(error_buffer, errmsg_ptr, ERRMSG_MAX_LEN);
                                *row_nums_ptr_buffer = *row_nums_ptr;

                                argv2[0] = bldrptr_buffer_for_wasm;  	// pass the buffer address for WASM space.
                                argv2[1] = bldr_size;					// the size of buffer
                                argv2[2] = ds_buffer_for_wasm;  	// pass the buffer address for WASM space.
                                argv2[3] = ds.length();					// the size of buffer
                                argv2[4] = qs_buffer_for_wasm;  	// pass the buffer address for WASM space.
                                argv2[5] = qs.length();	
                                argv2[6] = qp_buffer_for_wasm;  	// pass the buffer address for WASM space.
                                argv2[7] = qp.length();
                                argv2[8] = meta_buffer_for_wasm;  	// pass the buffer address for WASM space.
                                argv2[9] = meta_blob_size;	
                                argv2[10] = error_buffer_for_wasm;  	// pass the buffer address for WASM space.
                                argv2[11] = ERRMSG_MAX_LEN;
                                argv2[12] = row_nums_ptr_buffer_for_wasm;  	// pass the buffer address for WASM space.
                                argv2[13] = row_nums_size;

                                wasm_runtime_call_wasm(exec_env, func, 14, argv2);
                                if ((exception = wasm_runtime_get_exception(wasm_module_inst))) {
                                    CLS_LOG(20,"Got exception:%s",exception);
                                    return 0;
                                }
                                ret = argv2[0];
                                CLS_LOG(20,"Return value from wasm (ret) = %d",ret );
                                
                                wasm_runtime_module_free(wasm_module_inst, buffer_for_wasm);

                                 

                            }
                            errmsg.append(errmsg_ptr);
                            free(errmsg_ptr);
                            free(row_nums_ptr);

                            // debug only
                            CLS_LOG(20, "processSkyFbWASM errmsg=%s", errmsg.c_str());
                        }

                        if (ret != 0) {
                            CLS_ERR("ERROR: processSkyFb %s", errmsg.c_str());
                            CLS_ERR("ERROR: TablesErrCodes::%d", ret);
                            return -1;
                        }

                        createFbMeta(meta_builder,
                                     SFT_FLATBUF_FLEX_ROW,
                                     reinterpret_cast<unsigned char*>(
                                            result_builder.GetBufferPointer()),
                                     result_builder.GetSize());
                        CLS_LOG(20, "Created Buffer");
                        break;
                    }

                    case SFT_ARROW: {
                        std::shared_ptr<arrow::Table> table;
                        ret = processArrowCol(&table,
                                              data_schema,
                                              query_schema,
                                              query_preds,
                                              meta.blob_data,
                                              meta.blob_size,
                                              errmsg,
                                              row_nums);

                        if (ret != 0) {
                            CLS_ERR("ERROR: processArrow %s", errmsg.c_str());
                            CLS_ERR("ERROR: TablesErrCodes::%d", ret);
                            return -1;
                        }

                        std::shared_ptr<arrow::Buffer> buffer;
                        convert_arrow_to_buffer(table, &buffer);
                        createFbMeta(meta_builder,
                                     SFT_ARROW,
                                     reinterpret_cast<unsigned char*>(buffer->mutable_data()),
                                     buffer->size());
                        break;
                    }

                    case SFT_FLATBUF_UNION_ROW:
                    case SFT_FLATBUF_UNION_COL: {
                        flatbuffers::FlatBufferBuilder result_builder(1024);
                        int ret;

                        // extract ptr to meta data blob
                        const char* blob_dataptr = reinterpret_cast<const char*>(meta.blob_data);
                        size_t blob_sz           = meta.blob_size;

                        // extract bl_seq bufferlist
                        ceph::bufferlist bl_seq;
                        bl_seq.append(blob_dataptr, blob_sz);

                        // bl_seq for ROW format will only contain one bl
                        // bl_seq for COL format may contain multiple bls
                        ceph::bufferlist::iterator it_bl_seq = bl_seq.begin();

                        ceph::bufferlist bl;
                        ::decode(bl, it_bl_seq); // this decrements get_remaining by moving iterator
                        const char* dataptr = bl.c_str();
                        size_t datasz       = bl.length();

                        if(meta.blob_format == SFT_FLATBUF_UNION_ROW)
                            ret = processSkyFb_fbu_rows(
                                   result_builder,
                                   data_schema,
                                   query_schema,
                                   query_preds,
                                   dataptr,
                                   datasz,
                                   errmsg,
                                   row_nums);
                        else if(meta.blob_format == SFT_FLATBUF_UNION_COL)
                            ret = processSkyFb_fbu_cols(
                                    bl_seq,
                                    result_builder,
                                    data_schema,
                                    query_schema,
                                    query_preds,
                                    dataptr,
                                    datasz,
                                    errmsg,
                                    row_nums);
                        else
                            assert (Tables::TablesErrCodes::SkyFormatTypeNotRecognized==0);

                        if (ret != 0) {
                            CLS_ERR("ERROR: processSkyFb_fbu_* %s", errmsg.c_str());
                            CLS_ERR("ERROR: TablesErrCodes::%d", ret);
                            return -1;
                        }

                        createFbMeta(meta_builder,
                                     SFT_FLATBUF_FLEX_ROW,
                                     reinterpret_cast<unsigned char*>(
                                            result_builder.GetBufferPointer()),
                                     result_builder.GetSize());
                        break;
                    }
                    case SFT_FLATBUF_CSV_ROW:
                    case SFT_PG_TUPLE:
                    case SFT_CSV:
                    default:
                        assert (SkyFormatTypeNotRecognized==0);
                    }

                    // add meta_builder's data into a bufferlist as char*
                    bufferlist meta_bl;
                    meta_bl.append(reinterpret_cast<const char*>(       \
                                           meta_builder->GetBufferPointer()),
                                   meta_builder->GetSize());

                    // add this result into our results bl
                    ::encode(meta_bl, result_bl);
                    delete meta_builder;

                    if (op.index_read)
                        ds_rows_processed = row_nums.size();
                    else
                        ds_rows_processed = root_rows;
                    break;

                    // add this processed ds to sequence of bls and update counter
                    rows_processed += ds_rows_processed;
                }
                eval_ns += getns() - start;
            }
        }
    }

  // store timings and result set into output BL
  ::encode(read_ns, *out);
  ::encode(eval_ns, *out);
  ::encode(rows_processed, *out);
  ::encode(result_bl, *out);
  return 0;
}


/*
 * Older test method to process queries a through f
 */
static
int test_query_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    int ret = 0;
    uint64_t rows_processed = 0;
    uint64_t read_ns = 0;
    uint64_t eval_ns = 0;
    bufferlist result_bl;  // result set to be returned to client.
    test_op op;

    // extract the query op to get the query request params
    try {
        bufferlist::iterator it = in->begin();
        ::decode(op, it);
    } catch (const buffer::error &err) {
        CLS_ERR("ERROR: decoding query op");
        return -EINVAL;
    }

    // older processing here.
    bufferlist bl;
    if (op.query != "d" || !op.use_index) {
        uint64_t start = getns();
        ret = cls_cxx_read(hctx, 0, 0, &bl);  // read entire object.
        if (ret < 0) {
            CLS_ERR("ERROR: reading obj %d", ret);
            return ret;
        }
        read_ns = getns() - start;
    }
    result_bl.reserve(bl.length());

    // timing processing cost
    uint64_t start = getns();

    // our test data is fixed size per col and uses tpch lineitem schema.
    const size_t row_size = 141;
    const char *rows = bl.c_str();
    const size_t num_rows = bl.length() / row_size;
    rows_processed = num_rows;

    // offset of columns in a record
    const size_t order_key_field_offset = 0;
    const size_t line_number_field_offset = 12;
    const size_t quantity_field_offset = 16;
    const size_t extended_price_field_offset = 24;
    const size_t discount_field_offset = 32;
    const size_t shipdate_field_offset = 50;
    const size_t comment_field_offset = 97;
    const size_t comment_field_length = 44;

    if (op.query == "a") {  // count query on extended_price col
        size_t result_count = 0;
        for (size_t rid = 0; rid < num_rows; rid++) {
            const char *row = rows + rid * row_size;  // row off
            const char *vptr = row + extended_price_field_offset; // col off
            const double val = *(const double*)vptr;  // extract data as col type
            if (val > op.extended_price) {  // apply filter
                result_count++;  // counter of matching rows for this count(*) query
                // when a predicate passes, add some extra work
                add_extra_row_cost(op.extra_row_cost);
            }
        }
        ::encode(result_count, result_bl);  // store count into our result set.
    }
    else if (op.query == "b") {  // range query on extended_price col

        if (op.old_projection) {  // only add (orderkey,linenum) data to result set
            for (size_t rid = 0; rid < num_rows; rid++) {
                const char *row = rows + rid * row_size;
                const char *vptr = row + extended_price_field_offset;
                const double val = *(const double*)vptr;
                if (val > op.extended_price) {
                    result_bl.append(row + order_key_field_offset, 4);
                    result_bl.append(row + line_number_field_offset, 4);
                    add_extra_row_cost(op.extra_row_cost);
                }
            }
        }
        else {
            for (size_t rid = 0; rid < num_rows; rid++) {
                const char *row = rows + rid * row_size;
                const char *vptr = row + extended_price_field_offset;
                const double val = *(const double*)vptr;
                if (val > op.extended_price) {
                      result_bl.append(row, row_size);
                      add_extra_row_cost(op.extra_row_cost);
                }
            }
        }
    }
    else if (op.query == "c") {  // equality query on extended_price col

        if (op.old_projection) {
            for (size_t rid = 0; rid < num_rows; rid++) {
                const char *row = rows + rid * row_size;
                const char *vptr = row + extended_price_field_offset;
                const double val = *(const double*)vptr;
                if (val == op.extended_price) {
                  result_bl.append(row + order_key_field_offset, 4);
                  result_bl.append(row + line_number_field_offset, 4);
                  add_extra_row_cost(op.extra_row_cost);
                }
            }
        }
        else {
            for (size_t rid = 0; rid < num_rows; rid++) {
                const char *row = rows + rid * row_size;
                const char *vptr = row + extended_price_field_offset;
                const double val = *(const double*)vptr;
                if (val == op.extended_price) {
                  result_bl.append(row, row_size);
                  add_extra_row_cost(op.extra_row_cost);
                }
            }
        }
    }
    else if (op.query == "d") {// point query on PK (orderkey,linenum)

        if (op.use_index) {  // if we have previously built an index on the PK.
            // create the requested composite key from the op struct
            uint64_t key = ((uint64_t)op.order_key) << 32;
            key |= (uint32_t)op.line_number;
            const std::string strkey = Tables::u64tostr(key);

            // key lookup in omap to get the row offset
            bufferlist row_offset_bl;
            ret = cls_cxx_map_get_val(hctx, strkey, &row_offset_bl);
            if (ret < 0 && ret != -ENOENT) {
                CLS_ERR("cant read map val index %d", ret);
                return ret;
            }

            if (ret >= 0) {  // found key
                size_t row_offset;
                bufferlist::iterator it = row_offset_bl.begin();
                try {
                    ::decode(row_offset, it);
                } catch (const buffer::error &err) {
                    CLS_ERR("ERR cant decode index entry");
                    return -EIO;
                }

                size_t size;
                ret = cls_cxx_stat(hctx, &size, NULL);
                if (ret < 0) {
                    CLS_ERR("ERR stat %d", ret);
                    return ret;
                }
                // sanity check we won't try to read beyond obj size
                if ((row_offset + row_size) > size) {
                    return -EIO;
                }

                // read just the row
                bufferlist bl;
                ret = cls_cxx_read(hctx, row_offset, row_size, &bl);
                if (ret < 0) {
                    CLS_ERR("ERROR: reading obj %d", ret);
                    return ret;
                }

                if (bl.length() != row_size) {
                    CLS_ERR("unexpected read size");
                    return -EIO;
                }

                // create the row
                const char *row = bl.c_str();

                // add cols to result set.
                if (op.old_projection) {
                    result_bl.append(row + order_key_field_offset, 4);
                    result_bl.append(row + line_number_field_offset, 4);
                }
                else {
                    result_bl.append(row, row_size);
                }
            }

            // Count the num_keys stored in our index.
            // Not free but for now used as sanity check count, and currently
            // makes the simplifying assumption of unique keys, one per row.
            // TODO: is there a less expensive way to count just the number of keys
            // for a particular index in this object, perhaps prefix-based count?
            std::set<string> keys;
            bool more = true;
            const char *start_after = "\0";
            uint64_t max_to_get = keys.max_size();

            // TODO: sort/ordering needed to continue properly from start_after
            while (more) {
                ret = cls_cxx_map_get_keys(hctx, start_after, max_to_get, &keys, &more);
                if (ret < 0) {
                    CLS_ERR("cant read keys in index %d", ret);
                    return ret;
                }
                rows_processed += keys.size();
            }
        }
        else {  // no index, look for matching row(s) and extract key cols.
            if (op.old_projection) {
                for (size_t rid = 0; rid < num_rows; rid++) {
                    const char *row = rows + rid * row_size;
                    const char *vptr = row + order_key_field_offset;
                    const int order_key_val = *(const int*)vptr;
                    if (order_key_val == op.order_key) {
                        const char *vptr = row + line_number_field_offset;
                        const int line_number_val = *(const int*)vptr;
                        if (line_number_val == op.line_number) {
                            result_bl.append(row + order_key_field_offset, 4);
                            result_bl.append(row + line_number_field_offset, 4);
                            add_extra_row_cost(op.extra_row_cost);
                        }
                    }
                }
            }
            else { // no index, look for matching row(s) and extract all cols.
                for (size_t rid = 0; rid < num_rows; rid++) {
                    const char *row = rows + rid * row_size;
                    const char *vptr = row + order_key_field_offset;
                    const int order_key_val = *(const int*)vptr;
                    if (order_key_val == op.order_key) {
                        const char *vptr = row + line_number_field_offset;
                        const int line_number_val = *(const int*)vptr;
                        if (line_number_val == op.line_number) {
                            result_bl.append(row, row_size);
                            add_extra_row_cost(op.extra_row_cost);
                        }
                    }
                }
            }
        }
    }  // end query d
    else if (op.query == "e") {  // range query over multiple cols
        if (op.old_projection) {  // look for matching row(s) and extract key cols.
            for (size_t rid = 0; rid < num_rows; rid++) {
                const char *row = rows + rid * row_size;
                const int shipdate_val = *((const int *)(row + shipdate_field_offset));
                if (shipdate_val >= op.ship_date_low && shipdate_val < op.ship_date_high) {
                    const double discount_val = *((const double *)(row + discount_field_offset));
                    if (discount_val > op.discount_low && discount_val < op.discount_high) {
                        const double quantity_val = *((const double *)(row + quantity_field_offset));
                        if (quantity_val < op.quantity) {
                            result_bl.append(row + order_key_field_offset, 4);
                            result_bl.append(row + line_number_field_offset, 4);
                            add_extra_row_cost(op.extra_row_cost);
                        }
                    }
                }
            }
        }
        else { // look for matching row(s) and extract all cols.
            for (size_t rid = 0; rid < num_rows; rid++) {
                const char *row = rows + rid * row_size;
                const int shipdate_val = *((const int *)(row + shipdate_field_offset));
                if (shipdate_val >= op.ship_date_low && shipdate_val < op.ship_date_high) {
                    const double discount_val = *((const double *)(row + discount_field_offset));
                    if (discount_val > op.discount_low && discount_val < op.discount_high) {
                        const double quantity_val = *((const double *)(row + quantity_field_offset));
                        if (quantity_val < op.quantity) {
                            result_bl.append(row, row_size);
                            add_extra_row_cost(op.extra_row_cost);
                        }
                    }
                }
            }
        }
    }
    else if (op.query == "f") {  // regex query on comment cols
        if (op.old_projection) {  // look for matching row(s) and extract key cols.
            RE2 re(op.comment_regex);
            for (size_t rid = 0; rid < num_rows; rid++) {
                const char *row = rows + rid * row_size;
                const char *cptr = row + comment_field_offset;
                const std::string comment_val = string_ncopy(cptr,
                comment_field_length);
                if (RE2::PartialMatch(comment_val, re)) {
                    result_bl.append(row + order_key_field_offset, 4);
                    result_bl.append(row + line_number_field_offset, 4);
                    add_extra_row_cost(op.extra_row_cost);
                }
            }
        }
        else { // look for matching row(s) and extract all cols.
            RE2 re(op.comment_regex);
            for (size_t rid = 0; rid < num_rows; rid++) {
                const char *row = rows + rid * row_size;
                const char *cptr = row + comment_field_offset;
                const std::string comment_val = string_ncopy(cptr,
                comment_field_length);
                if (RE2::PartialMatch(comment_val, re)) {
                    result_bl.append(row, row_size);
                    add_extra_row_cost(op.extra_row_cost);
                }
            }
        }
    }
    else if (op.query == "fastpath") { // just copy data directly out.
        result_bl.append(bl);
    }
    else {
        return -EINVAL;
    }
    eval_ns += getns() - start;

    // store timings and result set into output BL
    ::encode(read_ns, *out);
    ::encode(eval_ns, *out);
    ::encode(rows_processed, *out);
    ::encode(result_bl, *out);
    return 0;
}

static
int exec_runstats_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    // unpack the requested op from the inbl.
    stats_op op;
    try {
        bufferlist::iterator it = in->begin();
        ::decode(op, it);
    } catch (const buffer::error &err) {
        CLS_ERR("ERROR: cls_tabular:exec_stats_op: decoding stats_op");
        return -EINVAL;
    }

    CLS_LOG(20, "exec_runstats_op: db_schema=%s", op.db_schema.c_str());
    CLS_LOG(20, "exec_runstats_op: table_name=%s", op.table_name.c_str());
    CLS_LOG(20, "exec_runstats_op: data_schema=%s", op.data_schema.c_str());

    using namespace Tables;
    std::string dbschema = op.db_schema;
    std::string table_name = op.table_name;
    schema_vec data_schema = schemaFromString(op.data_schema);

    return 0;
}


/*
 * Function: transform_db_op
 * Description: Method to convert database format.
 * @param[in] hctx    : CLS method context
 * @param[out] in     : input bufferlist
 * @param[out] out    : output bufferlist
 * Return Value: error code
*/
static
int transform_db_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    transform_op op;
    int offset = 0;

    // unpack the requested op from the inbl.
    try {
        bufferlist::iterator it = in->begin();
        ::decode(op, it);
    } catch (const buffer::error &err) {
        CLS_ERR("ERROR: cls_tabular:transform_db_op: decoding transform_op");
        return -EINVAL;
    }

    CLS_LOG(20, "transform_db_op: table_name=%s", op.table_name.c_str());
    CLS_LOG(20, "transform_db_op: transform_format_type=%d", op.required_type);

    // Columns specified in the query schmea will transformed and not the whole
    // object.
    Tables::schema_vec query_schema = Tables::schemaFromString(op.query_schema);

    // Object is sequence of actual data along with encoded metadata
    bufferlist encoded_meta_bls;

    // TODO: get individual off/len of fbmeta's inside obj and read one at a time.
    int ret = cls_cxx_read(hctx, 0, 0, &encoded_meta_bls);
    if (ret < 0) {
        CLS_ERR("ERROR: transform_db_op: reading obj. %d", ret);
        return ret;
    }

    using namespace Tables;
    ceph::bufferlist::iterator it = encoded_meta_bls.begin();
    while (it.get_remaining() > 0) {
        bufferlist bl;
        bufferlist transformed_encoded_meta_bl;
        try {
            ::decode(bl, it);  // unpack the next bl
        } catch (const buffer::error &err) {
            CLS_ERR("ERROR: decoding object format from BL");
            return -EINVAL;
        }

        // default usage here assumes the fbmeta is already in the bl
        sky_meta meta = getSkyMeta(&bl);
        std::string errmsg;

        // Check if transformation is required or not
        if (meta.blob_format == op.required_type) {
            // Source and destination object types are same, therefore no tranformation
            // is required.
            CLS_LOG(20, "No Transforming required");
            return 0;
        }

        // CREATE An FB_META, start with an empty builder first
        flatbuffers::FlatBufferBuilder *meta_builder =                  \
            new flatbuffers::FlatBufferBuilder();

        // According to the format type transform the object
        if (op.required_type == SFT_ARROW) {
            std::shared_ptr<arrow::Table> table;
            ret = transform_fb_to_arrow(meta.blob_data, meta.blob_size,
                                        query_schema, errmsg, &table);
            if (ret != 0) {
                CLS_ERR("ERROR: transforming object from flatbuffer to arrow");
                return ret;
            }

            // Convert arrow to a buffer
            std::shared_ptr<arrow::Buffer> buffer;
            convert_arrow_to_buffer(table, &buffer);

            createFbMeta(meta_builder,
                         SFT_ARROW,
                         reinterpret_cast<unsigned char*>(buffer->mutable_data()),
                         buffer->size());

        } else if (op.required_type == SFT_FLATBUF_FLEX_ROW) {
            flatbuffers::FlatBufferBuilder flatbldr(1024);  // pre-alloc sz

            ret = transform_arrow_to_fb(meta.blob_data, meta.blob_size, errmsg, flatbldr);
            if (ret != 0) {
                CLS_ERR("ERROR: transforming object from arrow to flatbuffer");
                return ret;
            }
            createFbMeta(meta_builder,
                         SFT_FLATBUF_FLEX_ROW,
                         reinterpret_cast<unsigned char*>(
                                 flatbldr.GetBufferPointer()),
                         flatbldr.GetSize());
        } else if (op.required_type == SFT_FLATBUF_UNION_ROW) {
            flatbuffers::FlatBufferBuilder flatbldr(1024);  // pre-alloc sz

            ret = transform_fbxrows_to_fbucols(meta.blob_data, meta.blob_size, errmsg, flatbldr);
            if (ret != 0) {
                CLS_ERR("ERROR: transforming object from fbxrows to fbucols");
                return ret;
            }
            createFbMeta(meta_builder,
                         SFT_FLATBUF_UNION_COL,
                         reinterpret_cast<unsigned char*>(
                                 flatbldr.GetBufferPointer()),
                         flatbldr.GetSize());
        }

        // Add meta_builder's data into a bufferlist as char*
        bufferlist meta_bl;
        meta_bl.append(reinterpret_cast<const char*>(                   \
                               meta_builder->GetBufferPointer()),
                       meta_builder->GetSize());
        ::encode(meta_bl, transformed_encoded_meta_bl);
        delete meta_builder;

        // Write the object back to Ceph. cls_cxx_replace truncates the original
        // object and writes full object.
        ret = cls_cxx_replace(hctx, offset, transformed_encoded_meta_bl.length(),
                              &transformed_encoded_meta_bl);
        if (ret < 0) {
            CLS_ERR("ERROR: writing obj full %d", ret);
            return ret;
        }
        offset += transformed_encoded_meta_bl.length();
    }
    return 0;
}


static
int example_query_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    // unpack the requested op from the inbl.
    inbl_sample_op op;
    try {
        bufferlist::iterator it = in->begin();
        ::decode(op, it);
    } catch (const buffer::error &err) {
        CLS_ERR("ERROR: cls_tabular:example_query_op: decoding inbl_sample_op");
        return -EINVAL;
    }

    std::string message = op.message;
    std::string instructions = op.instructions;
    int counter = op.counter;
    int func_id = op.func_id;

    CLS_LOG(20, "example_query_op: op.message = %s", message.c_str());
    CLS_LOG(20, "example_query_op: op.instructions = %s", instructions.c_str());
    CLS_LOG(20, "example_query_op: op.counter=%s", (std::to_string(counter)).c_str());
    CLS_LOG(20, "example_query_op: op.func_id=%s", (std::to_string(func_id)).c_str());

    using namespace Tables;
    //schema_vec data_schema = schemaFromString(op.data_schema);

    int64_t read_timer = getns();
    int64_t func_timer = 0;
    int rows_processed = 0;

    switch (func_id) {
        case 0:
            CLS_ERR("ERROR: cls_tabular:example_query_op: unknown func_id=%d", func_id);
            return -1;
            break;
        case 1: {
            uint64_t start = getns();
            rows_processed = example_func(counter);
            uint64_t end = getns();
            func_timer = end - start;
            CLS_LOG(20, "example_query_op: function result=%d", rows_processed);
            break;
        }
        default:
            CLS_ERR("ERROR: cls_tabular:example_query_op: unknown func_id %d", func_id);
            return -1;
    }

    // encode output info for client.
    outbl_sample_info info;
    info.message = "All done with example op.";
    info.rows_processed = rows_processed;
    info.read_time_ns = read_timer;
    info.eval_time_ns = func_timer;
    ::encode(info, *out);

    // encode result data for client.
    bufferlist result_bl;
    result_bl.append("result data goes into result bl.");
    ::encode(result_bl, *out);

    return 0;
}

static
int wasm_query_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    // unpack the requested op from the inbl.
    inbl_sample_op op;
    try {
        bufferlist::iterator it = in->begin();
        ::decode(op, it);
    } catch (const buffer::error &err) {
        CLS_ERR("ERROR: cls_tabular:example_query_op: decoding inbl_sample_op");
        return -EINVAL;
    }

    std::string message = op.message;
    std::string instructions = op.instructions;
    int counter = op.counter;
    int func_id = op.func_id;

    CLS_LOG(20, "wasm_query_op: op.message = %s", message.c_str());
    CLS_LOG(20, "wasm_query_op: op.instructions = %s", instructions.c_str());
    CLS_LOG(20, "wasm_query_op: op.counter=%s", (std::to_string(counter)).c_str());
    CLS_LOG(20, "wasm_query_op: op.func_id=%s", (std::to_string(func_id)).c_str());

    using namespace Tables;
    //schema_vec data_schema = schemaFromString(op.data_schema);

    int64_t read_timer = getns();
    int64_t func_timer = 0;
    int rows_processed = 0;

    switch (func_id) {
        case 0:
            CLS_ERR("ERROR: cls_tabular:wasm_query_op: unknown func_id=%d", func_id);
            return -1;
            break;
        case 1: {
            uint64_t start = getns();
            rows_processed = example_func(counter);
            uint64_t end = getns();
            func_timer = end - start;
            CLS_LOG(20, "wasm_query_op: function result=%d", rows_processed);
            break;
        }
        default:
            CLS_ERR("ERROR: cls_tabular:wasm_query_op: unknown func_id %d", func_id);
            return -1;
    }

    // encode output info for client.
    outbl_sample_info info;
    info.message = "All done with example op.";
    info.rows_processed = rows_processed;
    info.read_time_ns = read_timer;
    info.eval_time_ns = func_timer;
    ::encode(info, *out);

    // encode result data for client.
    bufferlist result_bl;
    result_bl.append("result data goes into result bl.");
    ::encode(result_bl, *out);

    return 0;
}


static
int hep_query_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    // unpack the requested op from the inbl.
    hep_op op;
    try {
        bufferlist::iterator it = in->begin();
        ::decode(op, it);
    } catch (const buffer::error &err) {
        CLS_ERR("ERROR: cls_tabular:hep_query_op: decoding inbl");
        return -EINVAL;
    }

    std::string dschema_str(op.data_schema);
    std::replace(dschema_str.begin(), dschema_str.end(), '\n', ';');
    std::string qschema_str(op.query_schema);
    std::replace(qschema_str.begin(), qschema_str.end(), '\n', ';');
    CLS_LOG(20, "hep_query_op: op.fastpath=%s", (std::to_string(op.fastpath)).c_str());
    CLS_LOG(20, "hep_query_op: op.dataset_name = %s", op.dataset_name.c_str());
    CLS_LOG(20, "hep_query_op: op.file_name = %s", op.file_name.c_str());
    CLS_LOG(20, "hep_query_op: op.tree_name = %s", op.tree_name.c_str());
    CLS_LOG(20, "hep_query_op: op.data_schema = %s", dschema_str.c_str());
    CLS_LOG(20, "hep_query_op: op.query_schema = %s", qschema_str.c_str());
    CLS_LOG(20, "hep_query_op: op.query_preds = %s", op.query_preds.c_str());

    bufferlist bl;
    int ret = cls_cxx_read(hctx, 0, 0, &bl);
    if (ret < 0) {
        CLS_ERR("ERROR: hep_query_op: reading obj.  ret=%d", ret);
        return ret;
    }

    // format op info into skyhook data structs
    Tables::schema_vec data_schema = \
        Tables::schemaFromString(op.data_schema);
    Tables::schema_vec query_schema = \
        Tables::schemaFromString(op.query_schema);
    Tables::predicate_vec query_preds = \
        Tables::predsFromString(data_schema, op.query_preds);

    char *data = bl.c_str();
    int data_size = bl.length();
    std::string errmsg;
    std::vector<unsigned int> row_nums;  // leave empty to process all rows.
    std::shared_ptr<arrow::Table> table;
    ret = Tables::processArrowColHEP(&table,
                                     data_schema,
                                     query_schema,
                                     query_preds,
                                     data,
                                     data_size,
                                     errmsg,
                                     row_nums);

    int cls_result_code = 0;
    bufferlist result_bl;
    CLS_LOG(20, "hep_query_op: processArrowCol errmsg=%s", errmsg.c_str());
    if (ret != 0) {
        if (ret == Tables::TablesErrCodes::NoInStorageProcessingRequired) {
            cls_result_code = Tables::TablesErrCodes::ClsResultCodeTrue;
            result_bl = bl;
        }
        else if (ret == Tables::TablesErrCodes::NoMatchingDataFound) {
            cls_result_code = Tables::TablesErrCodes::ClsResultCodeFalse;
            result_bl.append(" ");
        }
        else {
            CLS_ERR("ERROR: hep_query_op: processArrow failed: %s", errmsg.c_str());
            CLS_ERR("ERROR: hep_query_op: TablesErrCodes::%d", ret);
            return -1;
        }
    }

    // encode our results and return to client.
    ::encode(cls_result_code, *out);
    ::encode(result_bl, *out);

    return 0;
}

static int lock_obj_init_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    bool skipCreate = false;
    if (cls_cxx_stat(hctx, NULL, NULL) == 0)
        skipCreate=true;;

    lockobj_info op_in;

    try {
        bufferlist::iterator it = in->begin();
        ::decode(op_in, it);
    } catch (const buffer::error &err) {
        CLS_ERR("ERROR: cls_tabular: lock_obj_init_op: decoding inbl_lockobj_op");
        return -EINVAL;
    }

  // create/write the object
    if(!skipCreate) {
        int r = cls_cxx_write_full(hctx, in);
        if (r < 0)
          return r;
     }

     std::map<std::string, bufferlist> table_obj_map;
     int ret;

     table_obj_map[op_in.table_name]=*in;
     ret = cls_cxx_map_set_vals(hctx, &table_obj_map);

     if (ret < 0)
         return ret;

     bufferlist result_bl;
     ::encode(ret, *out);
     result_bl.append("Created Special Ceph Object");
     ::encode(result_bl, *out);
     return 0;
}

static int lock_obj_create_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    if (cls_cxx_stat(hctx, NULL, NULL) == 0)
        return -EEXIST;

    lockobj_info op;

    try {
      bufferlist::iterator it = in->begin();
      ::decode(op, it);
    } catch (const buffer::error &err) {
      CLS_ERR("ERROR: cls_tabular: lock_obj_create_op: decoding inbl_lockobj_op");
      return -EINVAL;
    }

  // create/write the object
    int r = cls_cxx_write_full(hctx, in);
    if (r < 0)
        return r;

    return 0;
  }
static
int lock_obj_free_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    lockobj_info op_in;

    try {
        bufferlist::iterator it = in->begin();
        ::decode(op_in, it);
    } catch (const buffer::error &err) {
        CLS_ERR("ERROR: cls_tabular: lock_obj_get_op: decoding inbl_lockobj_op");
        return -EINVAL;
    }

    std::string table_name = op_in.table_name;
    using namespace Tables;
    int ret;
    bufferlist bl_entry2;
    ret = cls_cxx_map_get_val(hctx, table_name, &bl_entry2);
    if (ret < 0)
      return ret;
    lockobj_info op_out;
    try {
        bufferlist::iterator it = bl_entry2.begin();
        ::decode(op_out, it);
    } catch (const buffer::error &err) {
        CLS_ERR("ERROR: cls_tabular:init_lock_obj_query_op: decoding inbl_lockobj_op");
        return -EINVAL;
    }
    CLS_LOG(20, "lock_obj_free_op 1: op_out.table_busy = %d", op_out.table_busy);
    CLS_LOG(20, "lock_obj_free_op 1: op_out.table_name=%s", op_out.table_name.c_str());
    std::map<std::string, bufferlist> table_obj_map;

    /* NOTE: If already free skip setting it here */
    if (op_out.table_busy) {
        op_out.table_busy = false;
        ::encode(op_out, *out);
        // TODO: encode new op and set
        table_obj_map[op_out.table_name] = *out;
        ret = cls_cxx_map_set_vals(hctx, &table_obj_map);
        if (ret < 0)
          return ret;
    }
    return 0;
}

static
int lock_obj_get_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    lockobj_info op_in;

    try {
        bufferlist::iterator it = in->begin();
        ::decode(op_in, it);
    } catch (const buffer::error &err) {
        CLS_ERR("ERROR: cls_tabular: lock_obj_get_op: decoding inbl_lockobj_op");
        return -EINVAL;
    }

    std::string table_name = op_in.table_name;
    int nobjs = op_in.num_objs;
    bool tableBusy = op_in.table_busy;

    CLS_LOG(20, "lock_obj_get_op: op.table_busy = %d", tableBusy);
    CLS_LOG(20, "lock_obj_get_op: op.numobjs=%d", nobjs);
    CLS_LOG(20, "lock_obj_get_op: op.table_name=%s", table_name.c_str());

    using namespace Tables;


    int ret;
    bufferlist bl_entry;
    ret = cls_cxx_map_get_val(hctx, table_name, &bl_entry);
    if (ret < 0)
      return ret;
    lockobj_info op_out;
    try {
        bufferlist::iterator it = bl_entry.begin();
        ::decode(op_out, it);
    } catch (const buffer::error &err) {
        CLS_ERR("ERROR: cls_tabular:init_lock_obj_query_op: decoding inbl_lockobj_op");
        return -EINVAL;
    }
    CLS_LOG(20, "lock_obj_get_op: op_out.table_busy = %d", op_out.table_busy);
    CLS_LOG(20, "lock_obj_get_op: op_out.table_name = %s", op_out.table_name.c_str());
    CLS_LOG(20, "lock_obj_get_op: op_out.table_group = %s", op_out.table_group.c_str());
    CLS_LOG(20, "lock_obj_get_op: op_out.nobjs = %d", op_out.num_objs);
    bufferlist result_bl;
    result_bl.append("result data goes into result bl.");
    ::encode(op_out, *out);
    return 0;
}

static
int lock_obj_acquire_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    lockobj_info op_in;

    try {
        bufferlist::iterator it = in->begin();
        ::decode(op_in, it);
    } catch (const buffer::error &err) {
        CLS_ERR("ERROR 1: cls_tabular:init_lock_obj_query_op: decoding inbl_lockobj_op");
        return -EINVAL;
    }

    std::string table_name = op_in.table_name;
    int nobjs = op_in.num_objs;
    bool tableBusy = op_in.table_busy;

    CLS_LOG(20, "lock_obj_acquire_op: op.table_busy = %d", tableBusy);
    CLS_LOG(20, "lock_obj_acquire_op: op.numobjs=%d", nobjs);

    using namespace Tables;
    int ret;

    bufferlist bl_entry;
    ret = cls_cxx_map_get_val(hctx, table_name, &bl_entry);

    if (ret < 0)
      return ret;

    lockobj_info op_out;
    try {
        bufferlist::iterator it = bl_entry.begin();
        ::decode(op_out, it);
    } catch (const buffer::error &err) {
        CLS_ERR("ERROR 2: cls_tabular:init_lock_obj_query_op: decoding init_lockobj_op");
        return -EINVAL;
    }

    CLS_LOG(20, "Acquire_lock_obj_query_op: op.table_busy = %d", op_out.table_busy);
    CLS_LOG(20, "Acquire_lock_obj_query_op: op.table_name = %s", op_out.table_name.c_str());
    CLS_LOG(20, "Acquire_lock_obj_query_op: op.table_group = %s", op_out.table_group.c_str());
    CLS_LOG(20, "Acquire_lock_obj_query_op: op.nobjs = %d", op_out.num_objs);

    if(!op_out.table_busy) {
        op_out.table_busy=!op_out.table_busy;
	bufferlist bflst;
	::encode(op_out, bflst);
        std::map<std::string, bufferlist> table_obj_map;
        table_obj_map[op_out.table_name]=bflst;
        ret = cls_cxx_map_set_vals(hctx, &table_obj_map);
    }
    else {
	// Can't acquire lock
	op_out.table_busy=false;
    }
    CLS_LOG(20, "Done setting up the values");
    ::encode(op_out, *out);
    return 0;
}
void __cls_init()
{
  CLS_LOG(20, "Loaded tabular class!");

  cls_register("tabular", &h_class);

  cls_register_cxx_method(h_class, "exec_query_op",
      CLS_METHOD_RD, exec_query_op, &h_exec_query_op);

  cls_register_cxx_method(h_class, "test_query_op",
      CLS_METHOD_RD, test_query_op, &h_test_query_op);

  cls_register_cxx_method(h_class, "example_query_op",
      CLS_METHOD_RD, example_query_op, &h_example_query_op);

  cls_register_cxx_method(h_class, "wasm_query_op",
      CLS_METHOD_RD, wasm_query_op, &h_wasm_query_op);

  cls_register_cxx_method(h_class, "hep_query_op",
      CLS_METHOD_RD, hep_query_op, &h_hep_query_op);

  cls_register_cxx_method(h_class, "exec_runstats_op",
      CLS_METHOD_RD | CLS_METHOD_WR, exec_runstats_op, &h_exec_runstats_op);

  cls_register_cxx_method(h_class, "build_index",
      CLS_METHOD_RD | CLS_METHOD_WR, build_index, &h_build_index);

  cls_register_cxx_method(h_class, "exec_build_sky_index_op",
      CLS_METHOD_RD | CLS_METHOD_WR, exec_build_sky_index_op, &h_exec_build_sky_index_op);

  cls_register_cxx_method(h_class, "transform_db_op",
      CLS_METHOD_RD | CLS_METHOD_WR, transform_db_op, &h_transform_db_op);

  cls_register_cxx_method(h_class, "lock_obj_init_op",
      CLS_METHOD_PROMOTE | CLS_METHOD_WR, lock_obj_init_op, &h_inittable_group_obj_query_op);

  cls_register_cxx_method(h_class, "lock_obj_free_op",
      CLS_METHOD_RD | CLS_METHOD_WR, lock_obj_free_op, &h_freelockobj_query_op);

  cls_register_cxx_method(h_class, "lock_obj_get_op",
      CLS_METHOD_RD | CLS_METHOD_WR, lock_obj_get_op, &h_getlockobj_query_op);

  cls_register_cxx_method(h_class, "lock_obj_acquire_op",
      CLS_METHOD_RD | CLS_METHOD_WR, lock_obj_acquire_op, &h_acquirelockobj_query_op);

  cls_register_cxx_method(h_class, "lock_obj_create_op",
      CLS_METHOD_RD | CLS_METHOD_WR, lock_obj_create_op, &h_createlockobj_query_op);
}

