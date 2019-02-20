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


CLS_VER(1,0)
CLS_NAME(tabular)

cls_handle_t h_class;
cls_method_handle_t h_query_op;
cls_method_handle_t h_build_index;
cls_method_handle_t h_build_sky_index;


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
int build_sky_index(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    // iterate over all fbs within an obj and create 2 indexes:
    // 1. for each fb, create idx_fb_entry (physical fb offset)
    // 2. for each row of an fb, create idx_rec_entry (logical row offset)

    // a ceph property encoding the len of each bl in front of the bl,
    // seems to be an int32 currently.
    const int ceph_bl_encoding_len = sizeof(int32_t);
    int fb_seq_num = 0;  // TODO: get this from a stable counter.

    std::string key_prefix;
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
        CLS_ERR("ERROR: cls_tabular:build_sky_idx: decoding idx_op");
        return -EINVAL;
    }
    Tables::schema_vec idx_schema = Tables::schemaFromString(op.idx_schema_str);

    // CLS_LOG(20,"%s",(op.toString()).c_str());

    // obj contains one bl that itself wraps a seq of encoded bls of skyhook fb
    bufferlist wrapped_bls;
    int ret = cls_cxx_read(hctx, 0, 0, &wrapped_bls);
    if (ret < 0) {
        CLS_ERR("ERROR: cls_tabular:build_sky_index: reading obj %d", ret);
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
        key_prefix = buildKeyPrefix(Tables::SIT_IDX_FB, root.schema_name,
                                    root.table_name);
        std::string sqnum = Tables::u64tostr(++fb_seq_num); // key data
        int len = sqnum.length();

        // create string of len min chars per type, int32 here
        int pos = len - 10;
        key_data = sqnum.substr(pos, len);

        // IDX_FB create the entry struct, encode into bufferlist
        bufferlist fb_bl;
        struct idx_fb_entry fb_ent(off, fb_len + ceph_bl_encoding_len);
        ::encode(fb_ent, fb_bl);
        key = key_prefix + key_data;
        fbs_index[key] = fb_bl;
        //CLS_LOG(20,"key=%s",(key+";"+fb_ent.toString()).c_str());

        // DATA CONTENT INDEXES (LOGICAL data reference):

        // Build the key prefixes for each index type (IDX_RID/IDX_REC/IDX_TXT)
        if (op.idx_type == Tables::SIT_IDX_RID) {
            key_prefix = buildKeyPrefix(Tables::SIT_IDX_RID, root.schema_name,
                                        root.table_name);
        }
        if (op.idx_type == Tables::SIT_IDX_REC or
            op.idx_type == Tables::SIT_IDX_TXT) {

            std::vector<std::string> keycols;
            for (auto it = idx_schema.begin(); it != idx_schema.end(); ++it) {
                keycols.push_back(it->name);
            }
            key_prefix = Tables::buildKeyPrefix(op.idx_type, root.schema_name,
                                                root.table_name, keycols);
        }

        // IDX_REC/IDX_RID/IDX_TXT: create the key data for each row
        for (uint32_t i = 0; i < root.nrows; i++) {
            Tables::sky_rec rec = Tables::getSkyRec(root.offs->Get(i));

            switch (op.idx_type) {
                case Tables::SIT_IDX_RID: {
                    // key_data is only the RID
                    key_data = Tables::u64tostr(rec.RID);

                    // create the entry, encode into bufferlist, update map
                    bufferlist rec_bl;
                    struct idx_rec_entry rec_ent(fb_seq_num, i, rec.RID);
                    ::encode(rec_ent, rec_bl);
                    key = key_prefix + key_data;
                    recs_index[key] = rec_bl;
                    //CLS_LOG(20,"kv=%s",(key+";"+rec_ent.toString()).c_str());
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
                    key = key_prefix + key_data;
                    recs_index[key] = rec_bl;
                    //CLS_LOG(20,"kv=%s",(key+";"+rec_ent.toString()).c_str());
                    break;
                }
                case Tables::SIT_IDX_TXT: {

                    // add each word in the row to a words vector, store as
                    // lower case and and preserve word sequence order.
                    std::vector<std::pair<std::string, int>> words;
                    std::string delims;
                    if (!op.idx_delims.empty())
                        delims = op.idx_delims;
                    else
                        delims = " \t\r\f\v\n"; // whitespace chars
                    auto row = rec.data.AsVector();
                    for (unsigned i = 0; i < idx_schema.size(); i++) {
                        if (i > 0) key_data += Tables::IDX_KEY_DELIM_INNER;
                        std::string line = \
                            row[idx_schema[i].idx].AsString().str();
                        boost::trim(line);
                        if (line.empty())
                            continue;
                        vector<std::string> elems;
                        boost::split(elems, line, boost::is_any_of(delims),
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
                        key = key_prefix + key_data;
                        txt_index[key] = txt_bl;
                        /*CLS_LOG(20,"kv=%s",
                                 (key+";"+txt_ent.toString()).c_str());*/
                    }
                    break;
                }
                default: {
                    CLS_ERR("build_sky_index() %s", (
                            "Index type unknown. type=" +
                            std::to_string(op.idx_type)).c_str());
                }
            }

            // IDX_REC/IDX_RID batch insert to omap (minimize IOs)
            if (recs_index.size() > op.idx_batch_size) {
                ret = cls_cxx_map_set_vals(hctx, &recs_index);
                if (ret < 0) {
                    CLS_ERR("error setting recs index entries %d", ret);
                    return ret;
                }
                recs_index.clear();
            }

            // IDX_TXT batch insert to omap (minimize IOs)
            if (txt_index.size() > op.idx_batch_size) {
                ret = cls_cxx_map_set_vals(hctx, &txt_index);
                if (ret < 0) {
                    CLS_ERR("error setting recs index entries %d", ret);
                    return ret;
                }
                txt_index.clear();
            }
        }  // end foreach row

        // IDX_FB batch insert to omap (minimize IOs)
        if (fbs_index.size() > op.idx_batch_size) {
            ret = cls_cxx_map_set_vals(hctx, &fbs_index);
            if (ret < 0) {
                CLS_ERR("error setting fbs index entries %d", ret);
                return ret;
            }
            fbs_index.clear();
        }
    }  // end while decode wrapped_bls


    // IDX_TXT insert remaining entries to omap
    if (txt_index.size() > 0) {
        ret = cls_cxx_map_set_vals(hctx, &txt_index);
        if (ret < 0) {
            CLS_ERR("error setting recs index entries %d", ret);
            return ret;
        }
        txt_index.clear();
    }

    // IDX_REC/IDX_RID insert remaining entries to omap
    if (recs_index.size() > 0) {
        ret = cls_cxx_map_set_vals(hctx, &recs_index);
        if (ret < 0) {
            CLS_ERR("error setting recs index entries %d", ret);
            return ret;
        }
    }
    // IDX_FB insert remaining entries to omap
    if (fbs_index.size() > 0) {
        ret = cls_cxx_map_set_vals(hctx, &fbs_index);
        if (ret < 0) {
            CLS_ERR("error setting fbs index entries %d", ret);
            return ret;
        }
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
get_idx_reads( 
    cls_method_context_t hctx, 
    std::map<int, struct Tables::read_info>& idx_reads, 
    bufferlist bl, 
    std::string db_schema, 
    std::string table ) {

    struct idx_rec_entry rec_ent;
    int ret=0;
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
    std::string key_prefix = buildKeyPrefix(Tables::SIT_IDX_FB,db_schema, table);
    std::string key_data = Tables::buildKeyData(Tables::SDT_INT32, rec_ent.fb_num);
    std::string key = key_prefix + key_data;
    struct idx_fb_entry fb_ent;
    bufferlist bl1;
    ret = cls_cxx_map_get_val(hctx, key, &bl1);
    if (ret < 0 && ret != -ENOENT) {
        CLS_ERR("cant read map val index for idx_fb_key, %d", ret);
        return ret;
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

bool check_predicate( Tables::predicate_vec index_preds, int opType )
{
    for (unsigned i = 0; i < index_preds.size(); i++) {

        if(index_preds[i]->opType() == opType)
            return true;
        }
    return false;

}

bool compare_keys( std::string user_key, std::string range_key )
{

    // Format of keys is like IDX_REC:*-LINEITEM:LINENUMBER-ORDERKEY:00000000000000000001-00000000000000000006
    // First splitting both the string on the basis of ':' delimiter
    vector<std::string> elem;
    boost::split(elem, user_key, boost::is_any_of( Tables::IDX_KEY_DELIM_OUTER ), boost::token_compress_on);

    vector<std::string> elem1;
    boost::split(elem1, range_key, boost::is_any_of( Tables::IDX_KEY_DELIM_OUTER ), boost::token_compress_on);
    
    // 4th entry in vector represents the value vector i.e after prefix
    vector<std::string> user_elem;
    vector<std::string> range_elem;

    // Now splitting on the basis of '-' delimiter and comparing first token of both
    boost::split(user_elem, elem[3] , boost::is_any_of( Tables::IDX_KEY_DELIM_INNER ), boost::token_compress_on);
    boost::split(range_elem, elem1[3], boost::is_any_of( Tables::IDX_KEY_DELIM_INNER ), boost::token_compress_on);

    
    if(user_elem[0] == range_elem[0])
        return true;
    return false;
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
    Tables::schema_vec index_schema,
    std::string db_schema,
    std::string table,
    int index_type,
    std::map<int, struct Tables::read_info>& idx_reads) {

    using namespace Tables;
    int ret = 0, ret2 = 0;

    // for each fb_seq_num, a corresponding read_info struct to
    // indicate the relevant rows within a given fb.
    // fb_seq_num is used as key, so that subsequent reads will always be from
    // a higher byte offset, if that matters.

    // create record key
    std::vector<std::string> keys;
    std::vector<std::string> index_cols = colnamesFromSchema(index_schema);
    std::string key_prefix = buildKeyPrefix(index_type,
                                            db_schema,
                                            table,
                                            index_cols);

    // build up the key data portion from the idx pred vals.
    std::string key_data;
    for (unsigned i = 0; i < index_preds.size(); i++) {
        uint64_t val;
        switch(index_preds[i]->colType()) {
            case SDT_INT8:
            case SDT_INT16:
            case SDT_INT32:
            case SDT_INT64: {
                TypedPredicate<int64_t>* p = \
                        dynamic_cast<TypedPredicate<int64_t>*>(index_preds[i]);
                val = static_cast<uint64_t>(p->Val());
                break;
            }
            default:
                assert (BuildSkyIndexUnsupportedColType==0);
        }
        if (i > 0)
            key_data += Tables::IDX_KEY_DELIM_INNER;
        key_data += buildKeyData(index_preds[i]->colType(), val);
    }
    std::string key = key_prefix + key_data;
    std::map<std::string, bufferlist> key_bf_map;
    if( !check_predicate(index_preds,SOT_lt) && !check_predicate(index_preds,SOT_gt) )
        keys.push_back(key);
    // Find the starting key for range query keys
    std::string start_after = "";
    if( check_predicate(index_preds,SOT_geq) || check_predicate(index_preds, SOT_gt))
    {
        start_after = key;
        CLS_LOG(20,"****Start_after key %s", start_after.c_str());
    }
    else if(check_predicate(index_preds,SOT_lt) || check_predicate(index_preds,SOT_leq) )
    {
        start_after = key_prefix;
    }
    bufferlist bl1;

    // Get keys-bufferlist two at a time and print row number/ offset detail
    if( start_after != "" ) {
        bool more = true;
        bool first = true;
        bool stop = false;
        int max_to_get = 2;
        while(first || key_bf_map.size() != 0)
        {
            ret2 = cls_cxx_map_get_vals(hctx, start_after, string() , max_to_get, &key_bf_map, &more);

            if (ret2 < 0 ) {
                CLS_ERR("cant read map val index rec for idx_rec key %d", ret2);
                return ret2;
            }
            if ( ret2 >= 0) {
            try {
                for(auto it=key_bf_map.cbegin(); it != key_bf_map.cend(); it++) {
                    const std::string& key1 = it->first;
                    
                    // Break if keyprefix in fetched key is not same as that passed by user 
                    if(key1.find(key_prefix) == std::string::npos) {
                        stop = true;
                        break;
                    }

                    // Know this is the last key, so update start_after value with it
                    if(std::next(it, 1) == key_bf_map.cend() ) {
                        start_after = key1;    
                        CLS_LOG(20,"%%key we have is=%s", key1.c_str());
                    }
                    bl1 = it->second;
                    // checking and breaking if key matches or exceeds key passed by the user in predicate column
                    if(check_predicate(index_preds,SOT_lt) || check_predicate(index_preds,SOT_leq) ) {
                        if( key_bf_map.find(key) != key_bf_map.end())
                        {
                            stop=true;
                            break;
                        }
                        else if( key1 > key )  // Special handling for leq predicate
                        {
                            if( check_predicate(index_preds, SOT_leq) && compare_keys(key, key1)) {
                                stop = false;
                            }
                            else {
                                stop = true;
                                break;
                            }
                        }
                    }
                    // Set the idx_reads info vector with the corresponding flatbuf off/len and row numbers for each matching record.
                    ret2 = get_idx_reads(hctx, idx_reads, bl1, db_schema, table);
                    if( ret2 < 0 )
                        return ret2;
                }
            } catch (const buffer::error &err) {
                    CLS_ERR("ERROR: decoding query idx_rec_ent");
                    return -EINVAL;

            }
            if(stop)
                break;
            first=false;
            }
        }

    }


    // lookup key in omap to get the row offset
    if (!keys.empty()) {
        for (unsigned i = 0; i < keys.size(); i++) {
            struct idx_rec_entry rec_ent;
            bufferlist bl;
            ret = cls_cxx_map_get_val(hctx, keys[i], &bl);

            if (ret < 0 && ret != -ENOENT) {
                CLS_ERR("cant read map val index rec for idx_rec key %d", ret);
                return ret;
            }
            if (ret >= 0) {
                // CLS_LOG(20,"idx_rec_ent FOUND for key=%s", key.c_str());
                ret2 = get_idx_reads(hctx, idx_reads, bl, db_schema, table);

                if( ret2 < 0)
                    return ret2;
            } else  {  // no rec found for key
                // CLS_LOG(20,"idx_rec_ent NOT FOUND for key=%s", key.c_str());
            }
        }
    }

    return 0;
}

/*
 * Primary method to process queries (new:flatbufs, old:q_a thru q_f)
 */
static int query_op_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
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
            schema_vec data_schema = schemaFromString(op.data_schema);

            // query_schema is the query schema
            schema_vec query_schema = schemaFromString(op.query_schema);

            // predicates to be applied, if any
            predicate_vec query_preds = predsFromString(data_schema, op.query_preds);

            // lookup correct flatbuf and potentially set specific row nums
            // to be processed next in processFb()
            if (op.index_read) {

                // index schema, should be empty if not op.index_read
                schema_vec index_schema = schemaFromString(op.index_schema);

                // index predicates to be applied, if any
                predicate_vec index_preds = predsFromString(data_schema, op.index_preds);

                // index lookup to set the read requests, if any rows match
                ret = read_sky_index(hctx,
                                      index_preds,
                                      index_schema,
                                      op.db_schema,
                                      op.table_name,
                                      op.index_type,
                                      idx1_reads);
                if (ret < 0) {
                    CLS_ERR("ERROR: do_index_lookup failed. %d", ret);
                    return ret;
                }

                if (op.index_plan_type == SIP_IDX_INTERSECTION or
                    op.index_plan_type == SIP_IDX_UNION) {

                    schema_vec index2_schema = \
                        schemaFromString(op.index2_schema);

                    predicate_vec index2_preds = \
                        predsFromString(data_schema, op.index2_preds);

                    ret = read_sky_index(hctx,
                                         index2_preds,
                                         index2_schema,
                                         op.db_schema,
                                         op.table_name,
                                         op.index2_type,
                                         idx2_reads);
                    if (ret < 0) {
                        CLS_ERR("ERROR: do_index2_lookup failed. %d", ret);
                        return ret;
                    }

                    // INDEX PLAN (INTERSECTION or UNION)
                    // for each fbseq_num in idx1 reads, check if idx2 has any
                    // rows for same fbseq_num, perform intersection or union
                    // of rows, store resulting rows into our reads vector.
                    reads.clear();
                    for (auto it1=idx1_reads.begin(); it1!=idx1_reads.end();
                         ++it1) {

                        int fbnum = it1->first;
                        auto it2 = idx2_reads.find(fbnum);
                        if (it2!=idx2_reads.end()) {
                            struct Tables::read_info ri1 = it1->second;
                            struct Tables::read_info ri2 = it2->second;
                            std::vector<unsigned int> rnums1 = ri1.rnums;
                            std::vector<unsigned int> rnums2 = ri2.rnums;
                            std::sort(rnums1.begin(), rnums1.end());
                            std::sort(rnums2.begin(), rnums2.end());
                            std::vector<unsigned> result_rnums(rnums1.size() +
                                                               rnums2.size());

                            if (op.index_plan_type == SIP_IDX_INTERSECTION) {
                                auto it = std::set_intersection(
                                                        rnums1.begin(),
                                                        rnums1.end(),
                                                        rnums2.begin(),
                                                        rnums2.end(),
                                                        result_rnums.begin());
                                result_rnums.resize(it - result_rnums.begin());
                            }
                            if (op.index_plan_type == SIP_IDX_UNION) {
                                auto it = std::set_union(
                                                        rnums1.begin(),
                                                        rnums1.end(),
                                                        rnums2.begin(),
                                                        rnums2.end(),
                                                        result_rnums.begin());
                                result_rnums.resize(it - result_rnums.begin());
                            }
                            if (!result_rnums.empty()) {
                                reads[fbnum] = Tables::read_info(
                                                                ri1.fb_seq_num,
                                                                ri1.off,
                                                                ri1.len,
                                                                result_rnums);
                            }
                        }
                    }
                }
                if (op.index_plan_type == SIP_IDX_STANDARD) {  // default
                    reads = idx1_reads;  // only 1 index was read
                }
            } else {

                // just read entire object.
                // fbseq_num=0, off=0, len=0 indicates read entire obj/all fbs
                reads.clear();
                struct read_info ri(0, 0, 0, {});
                reads[0] = ri;
            }

            // now we can decode and process each bl in the obj, specified
            // by each read request.
            // NOTE: 1 bl contains exactly 1 flatbuf.
            // weak ordering in map will iterate over fb nums in sequence
            for (auto it=reads.begin(); it!=reads.end(); ++it) {
                bufferlist b;
                size_t off = it->second.off;
                size_t len = it->second.len;
                std::vector<unsigned int> row_nums = it->second.rnums;

                uint64_t start = getns();
                ret = cls_cxx_read(hctx, off, len, &b);
                if (ret < 0) {
                  CLS_ERR("ERROR: reading flatbuf obj %d", ret);
                  return ret;
                }
                read_ns += getns() - start;

                start = getns();
                ceph::bufferlist::iterator it2 = b.begin();
                while (it2.get_remaining() > 0) {
                    bufferlist bl;
                    try {
                        ::decode(bl, it2);  // unpack the next bl (flatbuf)
                    } catch (const buffer::error &err) {
                        CLS_ERR("ERROR: decoding flatbuf from BL");
                        return -EINVAL;
                    }

                    // get our data as contiguous bytes before accessing as flatbuf
                    const char* fb = bl.c_str();
                    size_t fb_size = bl.length();

                    sky_root root = Tables::getSkyRoot(fb, fb_size);
                    flatbuffers::FlatBufferBuilder flatbldr(1024);  // pre-alloc sz
                    std::string errmsg;
                    ret = processSkyFb(flatbldr,
                                       data_schema,
                                       query_schema,
                                       query_preds,
                                       fb,
                                       fb_size,
                                       errmsg,
                                       row_nums);

                    if (ret != 0) {
                        CLS_ERR("ERROR: processing flatbuf, %s", errmsg.c_str());
                        CLS_ERR("ERROR: TablesErrCodes::%d", ret);
                        return -1;
                    }
                    if (op.index_read)
                        rows_processed += row_nums.size();
                    else
                        rows_processed += root.nrows;
                    const char *processed_fb = \
                        reinterpret_cast<char*>(flatbldr.GetBufferPointer());
                    int bufsz = flatbldr.GetSize();

                    // add this processed fb to our sequence of bls
                    bufferlist ans;
                    ans.append(processed_fb, bufsz);
                    ::encode(ans, result_bl);
                }
                eval_ns += getns() - start;
            }
        }
    } else {
      // older processing here.
      bufferlist bl;
      if (op.query != "d" || !op.use_index) {
        uint64_t start = getns();
        int ret = cls_cxx_read(hctx, 0, 0, &bl);  // read entire object.
        if (ret < 0) {
          CLS_ERR("ERROR: reading obj %d", ret);
          return ret;
        }
        read_ns = getns() - start;
      }
      result_bl.reserve(bl.length());

      uint64_t start = getns();

      // our test data is fixed size per col and uses tpch lineitem schema.
      const size_t row_size = 141;
      const char *rows = bl.c_str();
      const size_t num_rows = bl.length() / row_size;
      rows_processed = num_rows;

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
      } else if (op.query == "b") {  // range query on extended_price col
        if (op.projection) {  // only add (orderkey,linenum) data to result set
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
        } else {
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
      } else if (op.query == "c") {  // equality query on extended_price col
        if (op.projection) {
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
        } else {
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
      } else if (op.query == "d") {// point query on PK (orderkey,linenum)
        if (op.use_index) {  // if we have previously built an index on the PK.
          // create the requested composite key from the op struct
          uint64_t key = ((uint64_t)op.order_key) << 32;
          key |= (uint32_t)op.line_number;
          const std::string strkey = Tables::u64tostr(key);

          // key lookup in omap to get the row offset
          bufferlist row_offset_bl;
          int ret = cls_cxx_map_get_val(hctx, strkey, &row_offset_bl);
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
            int ret = cls_cxx_stat(hctx, &size, NULL);
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

            const char *row = bl.c_str();

            // add to result set.
            if (op.projection) {
              result_bl.append(row + order_key_field_offset, 4);
              result_bl.append(row + line_number_field_offset, 4);
            } else {
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
            if (ret < 0 ) {
                CLS_ERR("cant read keys in index %d", ret);
                return ret;
            }
            rows_processed += keys.size();
        }

        } else {  // no index, look for matching row(s) and extract key cols.
          if (op.projection) {
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
          } else { // no index, look for matching row(s) and extract all cols.
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
      } else if (op.query == "e") {  // range query over multiple cols
        if (op.projection) {  // look for matching row(s) and extract key cols.
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
        } else { // look for matching row(s) and extract all cols.
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
      } else if (op.query == "f") {  // regex query on comment cols
        if (op.projection) {  // look for matching row(s) and extract key cols.
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
        } else { // look for matching row(s) and extract all cols.
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
      } else if (op.query == "fastpath") { // just copy data directly out.
            result_bl.append(bl);
      } else {
        return -EINVAL;
      }
      eval_ns += getns() - start;
    }

  // store timings and result set into output BL
  ::encode(read_ns, *out);
  ::encode(eval_ns, *out);
  ::encode(rows_processed, *out);
  ::encode(result_bl, *out);
  return 0;
}

void __cls_init()
{
  CLS_LOG(20, "Loaded tabular class!");

  cls_register("tabular", &h_class);

  cls_register_cxx_method(h_class, "query_op",
      CLS_METHOD_RD, query_op_op, &h_query_op);

  cls_register_cxx_method(h_class, "build_index",
      CLS_METHOD_RD | CLS_METHOD_WR, build_index, &h_build_index);

  cls_register_cxx_method(h_class, "build_sky_index",
      CLS_METHOD_RD | CLS_METHOD_WR, build_sky_index, &h_build_sky_index);
}
