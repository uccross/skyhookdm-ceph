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
cls_method_handle_t h_exec_query_op;
cls_method_handle_t h_exec_runstats_op;
cls_method_handle_t h_build_index;
cls_method_handle_t h_exec_build_sky_index_op;
cls_method_handle_t h_transform_db_op;


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
    if( ret < 0 ) {
        return ret;
    }
    return 0;
}

// Get sky_layout_type from xattr, if not present set to Flatbuffer
static
int get_sky_layout_type(cls_method_context_t hctx, int& layout_type) {

    bufferlist bl;
    int ret = cls_cxx_getxattr(hctx, "sky_layout_type", &bl);
    if (ret < 0) {
        return ret;
    }
    else {
        try {
            bufferlist::iterator it = bl.begin();
            ::decode(layout_type, it);
        } catch (const buffer::error &err) {
            CLS_ERR("ERROR: cls_tabular:get_sky_layout_type: decoding layout_type");
            return -EINVAL;
        }
    }
    return 0;

}

// Set object type to xattr
static
int set_sky_layout_type(cls_method_context_t hctx, int layout_type) {

    bufferlist bl;
    ::encode(layout_type, bl);
    int ret = cls_cxx_setxattr(hctx, "sky_layout_type", &bl);
    if( ret < 0 ) {
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
        key_fb_prefix = buildKeyPrefix(Tables::SIT_IDX_FB, root.db_schema,
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
                                             root.db_schema,
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
                                                     root.db_schema,
                                                     root.table_name,
                                                     keycols);
        }

        // IDX_REC/IDX_RID/IDX_TXT: create the key data for each row
        for (uint32_t i = 0; i < root.nrows; i++) {
            Tables::sky_rec rec = Tables::getSkyRec(root.offs->Get(i));

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
        if (ret < 0 ) {
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
    if (ret < 0 && ret != -ENOENT ) {
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
    if (ret < 0 && ret != -ENOENT ) {
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

        if (ret2 < 0 && ret2 != -ENOENT ) {
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
 * Primary method to process queries (new:flatbufs, old:q_a thru q_f)
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

            // required for index plan or scan plan if index plan not chosen.
            predicate_vec index_preds;
            predicate_vec index2_preds;

            std::string key_fb_prefix = buildKeyPrefix(SIT_IDX_FB,
                                                       op.db_schema,
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
                                       op.db_schema,
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
                                       op.db_schema,
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
                        CLS_LOG(20,
                            "exec_query_op: WARN: No FBs index entries found.");

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
                std::string msg = "off=" + std::to_string(off)
                                        + ";len=" + std::to_string(len);
                CLS_LOG(20, "exec_query_op: READING %s", msg.c_str());
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

using namespace Tables;

#define RETURN_ON_FAILURE(expr)                                 \
    do {                                                        \
        arrow::Status status_ = (expr);                         \
        if (!status_.ok()) {                                    \
            CLS_ERR("ERROR: %s", status_.message().c_str());    \
            return -EINVAL;                                     \
        }                                                       \
    } while (0);

static int print_arrowbuf_colwise(std::shared_ptr<arrow::Table>& table)
{
    std::vector<std::shared_ptr<arrow::Array>> array_list;

    /* From Table get the schema and from schema get the skyhook schema
     * which is stored as a metadata */
    auto schema = table->schema();
    auto metadata = schema->metadata();
    schema_vec sc = schemaFromString(metadata->value(METADATA_DATA_SCHEMA));

    // Iterate through each column in print the data inside it
    for (auto it = sc.begin(); it != sc.end(); ++it) {
        col_info col = *it;
        std::string col_str (table->column(col.idx)->name());
        col_str.append(" | ");
        std::vector<std::shared_ptr<arrow::Array>> array_list = table->column(col.idx)->data()->chunks();

        switch(col.type) {
            case SDT_BOOL: {
                for (auto it = array_list.begin(); it != array_list.end(); ++it) {
                    auto array = *it;
                    for (int j = 0; j < array->length(); j++) {
                        col_str.append(std::to_string(std::static_pointer_cast<arrow::BooleanArray>(array)->Value(j)));
                        col_str.append(" | ");
                    }
                }
                break;
            }
            case SDT_INT8: {
                for (auto it = array_list.begin(); it != array_list.end(); ++it) {
                    auto array = *it;
                    for (int j = 0; j < array->length(); j++) {
                        col_str.append(std::to_string(std::static_pointer_cast<arrow::Int8Array>(array)->Value(j)));
                        col_str.append(" | ");
                    }
                }
                break;
            }
            case SDT_INT16: {
                for (auto it = array_list.begin(); it != array_list.end(); ++it) {
                    auto array = *it;
                    for (int j = 0; j < array->length(); j++) {
                        col_str.append(std::to_string(std::static_pointer_cast<arrow::Int16Array>(array)->Value(j)));
                        col_str.append(" | ");
                    }
                }
            }
            case SDT_INT32: {
                for (auto it = array_list.begin(); it != array_list.end(); ++it) {
                    auto array = *it;
                    for (int j = 0; j < array->length(); j++) {
                        col_str.append(std::to_string(std::static_pointer_cast<arrow::Int32Array>(array)->Value(j)));
                        col_str.append(" | ");
                    }
                }
                break;
            }
            case SDT_INT64: {
                for (auto it = array_list.begin(); it != array_list.end(); ++it) {
                    auto array = *it;
                    for (int j = 0; j < array->length(); j++) {
                        col_str.append(std::to_string(std::static_pointer_cast<arrow::Int64Array>(array)->Value(j)));
                        col_str.append(" | ");
                    }
                }
                break;
            }
            case SDT_UINT8: {
                for (auto it = array_list.begin(); it != array_list.end(); ++it) {
                    auto array = *it;
                    for (int j = 0; j < array->length(); j++) {
                        col_str.append(std::to_string(std::static_pointer_cast<arrow::UInt8Array>(array)->Value(j)));
                        col_str.append(" | ");
                    }
                }
                break;
            }
            case SDT_UINT16: {
                for (auto it = array_list.begin(); it != array_list.end(); ++it) {
                    auto array = *it;
                    for (int j = 0; j < array->length(); j++) {
                        col_str.append(std::to_string(std::static_pointer_cast<arrow::UInt16Array>(array)->Value(j)));
                        col_str.append(" | ");
                    }
                }
                break;
            }
            case SDT_UINT32: {
                for (auto it = array_list.begin(); it != array_list.end(); ++it) {
                    auto array = *it;
                    for (int j = 0; j < array->length(); j++) {
                        col_str.append(std::to_string(std::static_pointer_cast<arrow::UInt32Array>(array)->Value(j)));
                        col_str.append(" | ");
                    }
                }
                break;
            }
            case SDT_UINT64: {
                for (auto it = array_list.begin(); it != array_list.end(); ++it) {
                    auto array = *it;
                    for (int j = 0; j < array->length(); j++) {
                        col_str.append(std::to_string(std::static_pointer_cast<arrow::UInt64Array>(array)->Value(j)));
                        col_str.append(" | ");
                    }
                }
                break;
            }
            case SDT_CHAR: {
                for (auto it = array_list.begin(); it != array_list.end(); ++it) {
                    auto array = *it;
                    for (int j = 0; j < array->length(); j++) {
                        col_str.append(std::to_string(std::static_pointer_cast<arrow::Int8Array>(array)->Value(j)));
                        col_str.append(" | ");
                    }
                }
                break;
            }
            case SDT_UCHAR: {
                for (auto it = array_list.begin(); it != array_list.end(); ++it) {
                    auto array = *it;
                    for (int j = 0; j < array->length(); j++) {
                        col_str.append(std::to_string(std::static_pointer_cast<arrow::UInt8Array>(array)->Value(j)));
                        col_str.append(" | ");
                    }
                }
                break;
            }
            case SDT_FLOAT: {
                for (auto it = array_list.begin(); it != array_list.end(); ++it) {
                    auto array = *it;
                    for (int j = 0; j < array->length(); j++) {
                        col_str.append(std::to_string(std::static_pointer_cast<arrow::FloatArray>(array)->Value(j)));
                        col_str.append(" | ");
                    }
                }
                break;
            }
            case SDT_DOUBLE: {
                for (auto it = array_list.begin(); it != array_list.end(); ++it) {
                    auto array = *it;
                    for (int j = 0; j < array->length(); j++) {
                        col_str.append(std::to_string(std::static_pointer_cast<arrow::DoubleArray>(array)->Value(j)));
                        col_str.append(" | ");
                    }
                }
                break;
            }
            case SDT_DATE:
            case SDT_STRING: {
                for (auto it = array_list.begin(); it != array_list.end(); ++it) {
                    auto array = *it;
                    for (int j = 0; j < array->length(); j++) {
                        col_str.append(std::static_pointer_cast<arrow::StringArray>(array)->GetString(j));
                        col_str.append(" | ");
                    }
                }
                break;
            }
            default: {
                CLS_LOG(20, "Schema Unsupported type %s", std::to_string(col.type).c_str());
                return TablesErrCodes::UnsupportedSkyDataType;
            }
        }
        CLS_LOG(20, "%s", col_str.c_str());
    }
    return 0;
}

//TODO: Update this function to print all the chunked arrays
static int print_arrowbuf_rowwise(std::shared_ptr<arrow::Table>& table)
{
    std::vector<std::shared_ptr<arrow::Array>> array_list;
    std::string schema_str;

    /* From Table get the schema and from schema get the skyhook schema
     * which is stored as a metadata */
    auto schema = table->schema();
    auto metadata = schema->metadata();
    schema_vec sc = schemaFromString(metadata->value(METADATA_DATA_SCHEMA));
    int num_rows = std::stoi(metadata->value(METADATA_NUM_ROWS));

    // Get the names of each column and create required array vector
    for (auto it = sc.begin(); it != sc.end(); ++it) {
        col_info col = *it;
        schema_str.append(table->column(col.idx)->name());
        schema_str.append(" | ");

        // Create the builder instances for repsective datatypes
        switch(col.type) {
            case SDT_BOOL: {
                array_list.emplace_back(std::static_pointer_cast<arrow::BooleanArray>(table->column(col.idx)->data()->chunk(0)));
                break;
            }
            case SDT_INT8: {
                array_list.emplace_back(std::static_pointer_cast<arrow::Int8Array>(table->column(col.idx)->data()->chunk(0)));
                break;
            }
            case SDT_INT16: {
                array_list.emplace_back(std::static_pointer_cast<arrow::Int16Array>(table->column(col.idx)->data()->chunk(0)));
                break;
            }
            case SDT_INT32: {
                array_list.emplace_back(std::static_pointer_cast<arrow::Int32Array>(table->column(col.idx)->data()->chunk(0)));
                break;
            }
            case SDT_INT64: {
                array_list.emplace_back(std::static_pointer_cast<arrow::Int64Array>(table->column(col.idx)->data()->chunk(0)));
                break;
            }
            case SDT_UINT8: {
                array_list.emplace_back(std::static_pointer_cast<arrow::UInt8Array>(table->column(col.idx)->data()->chunk(0)));
                break;
            }
            case SDT_UINT16: {
                array_list.emplace_back(std::static_pointer_cast<arrow::UInt16Array>(table->column(col.idx)->data()->chunk(0)));
                break;
            }
            case SDT_UINT32: {
                array_list.emplace_back(std::static_pointer_cast<arrow::UInt32Array>(table->column(col.idx)->data()->chunk(0)));
                break;
            }
            case SDT_UINT64: {
                array_list.emplace_back(std::static_pointer_cast<arrow::UInt64Array>(table->column(col.idx)->data()->chunk(0)));
                break;
            }
            case SDT_CHAR: {
                array_list.emplace_back(std::static_pointer_cast<arrow::Int8Array>(table->column(col.idx)->data()->chunk(0)));
                break;
            }
            case SDT_UCHAR: {
                array_list.emplace_back(std::static_pointer_cast<arrow::UInt8Array>(table->column(col.idx)->data()->chunk(0)));
                break;
            }
            case SDT_FLOAT: {
                array_list.emplace_back(std::static_pointer_cast<arrow::FloatArray>(table->column(col.idx)->data()->chunk(0)));
                break;
            }
            case SDT_DOUBLE: {
                array_list.emplace_back(std::static_pointer_cast<arrow::DoubleArray>(table->column(col.idx)->data()->chunk(0)));
                break;
            }
            case SDT_DATE:
            case SDT_STRING: {
                array_list.emplace_back(std::static_pointer_cast<arrow::StringArray>(table->column(col.idx)->data()->chunk(0)));
                break;
            }
            default: {
                CLS_LOG(20, "Schema Unsupported type %s", std::to_string(col.type).c_str());
                return TablesErrCodes::UnsupportedSkyDataType;
            }
        }
    }
    CLS_LOG(20, "%s", schema_str.c_str());

    for (int i = 0; i < num_rows; i++) {
        std::string row_str;

        // For this row get the data from each columns
        for (auto it = sc.begin(); it != sc.end(); ++it) {
            col_info col = *it;
            auto array = array_list[std::distance(sc.begin(), it)];
            switch(col.type) {
                case SDT_BOOL: {
                    row_str.append(std::to_string(std::static_pointer_cast<arrow::BooleanArray>(array)->Value(i)));
                    break;
                }
                case SDT_INT8: {
                    row_str.append(std::to_string(std::static_pointer_cast<arrow::Int8Array>(array)->Value(i)));
                    break;
                }
                case SDT_INT16: {
                    row_str.append(std::to_string(std::static_pointer_cast<arrow::Int16Array>(array)->Value(i)));
                    break;
                }
                case SDT_INT32: {
                    row_str.append(std::to_string(std::static_pointer_cast<arrow::Int32Array>(array)->Value(i)));
                    break;
                }
                case SDT_INT64: {
                    row_str.append(std::to_string(std::static_pointer_cast<arrow::Int64Array>(array)->Value(i)));
                    break;
                }
                case SDT_UINT8: {
                    row_str.append(std::to_string(std::static_pointer_cast<arrow::UInt8Array>(array)->Value(i)));
                    break;
                }
                case SDT_UINT16: {
                    row_str.append(std::to_string(std::static_pointer_cast<arrow::UInt16Array>(array)->Value(i)));
                    break;
                }
                case SDT_UINT32: {
                    row_str.append(std::to_string(std::static_pointer_cast<arrow::UInt32Array>(array)->Value(i)));
                    break;
                }
                case SDT_UINT64: {
                    row_str.append(std::to_string(std::static_pointer_cast<arrow::UInt64Array>(array)->Value(i)));
                    break;
                }
                case SDT_CHAR: {
                    row_str.push_back((char)std::static_pointer_cast<arrow::Int8Array>(array)->Value(i));
                    break;
                }
                case SDT_UCHAR: {
                    row_str.push_back((char)std::static_pointer_cast<arrow::UInt8Array>(array)->Value(i));
                    break;
                }
                case SDT_FLOAT: {
                    row_str.append(std::to_string(std::static_pointer_cast<arrow::FloatArray>(array)->Value(i)));
                    break;
                }
                case SDT_DOUBLE: {
                    row_str.append(std::to_string(std::static_pointer_cast<arrow::DoubleArray>(array)->Value(i)));
                    break;
                }
                case SDT_DATE:
                case SDT_STRING: {
                    row_str.append(std::static_pointer_cast<arrow::StringArray>(array)->GetString(i));
                    break;
                }
                default: {
                    CLS_LOG(20, "Row Unsupported type %s", std::to_string(col.type).c_str());
                    return TablesErrCodes::UnsupportedSkyDataType;
                }
            }
            row_str.append(" | ");
        }
        CLS_LOG(20, "%s", row_str.c_str());
    }
    return 0;
}


/* @todo: This is a temporary function to demonstrate buffer is read from the file.
 * In reality, Ceph will return a bufferlist containing a buffer.
 */
int read_from_file(const char *filename, std::shared_ptr<arrow::Buffer> *buffer)
{
  // Open file
  std::ifstream infile(filename);

  // Get length of file
  infile.seekg(0, infile.end);
  size_t length = infile.tellg();
  infile.seekg(0, infile.beg);

  std::unique_ptr<char[]> cdata{new char [length + 1]};

  // Read file
  infile.read(cdata.get(), length);
  std::string data(cdata.get(), length);
  arrow::Buffer::FromString(data, buffer);
  return 0;
}

/* @todo: This is a temporary function to demonstrate buffer is written on to a file.
 * In reality, the buffer is given to Ceph which takes care of writing.
 */
int write_to_file(const char *filename, arrow::Buffer* buffer)
{
    std::ofstream ofile("/tmp/skyhook.arrow");
    std::string str(buffer->ToString());
    ofile.write(buffer->ToString().c_str(), buffer->size());
    return 0;
}

/*
 * Function: extract_arrow_from_buffer
 * Description: Extract arrow table from a buffer.
 *  a. Where to read - In this case we are using buffer, but it can be streams or
 *                     files as well.
 *  b. InputStream - We connect a buffer (or file) to the stream and reader will read
 *                   data from this stream. We are using arrow::InputStream as an
 *                   input stream.
 *  c. Reader - Reads the data from InputStream. We are using arrow::RecordBatchReader
 *              which will read the data from input stream.
 * @param[out] table  : Arrow table to be converted
 * @param[in] buffer  : Input buffer
 * Return Value: error code
 */
static
int extract_arrow_from_buffer(std::shared_ptr<arrow::Table>* table, const std::shared_ptr<arrow::Buffer> &buffer)
{
    CLS_LOG(20, "Reading arrow from a buffer");

    // Initialization related to reading from a buffer
    const std::shared_ptr<arrow::io::InputStream> buf_reader = std::make_shared<arrow::io::BufferReader>(buffer);
    std::shared_ptr<arrow::ipc::RecordBatchReader> reader;
    arrow::ipc::RecordBatchStreamReader::Open(buf_reader, &reader);

    // Initilaization related to read to apache arrow
    std::vector<std::shared_ptr<arrow::RecordBatch>> batch_vec;
    while (true){
        std::shared_ptr<arrow::RecordBatch> chunk;
        reader->ReadNext(&chunk);
        if (chunk == nullptr) break;
        batch_vec.push_back(chunk);
    }

    arrow::Table::FromRecordBatches(batch_vec, table);
    return 0;
}

/*
 * Function: convert_arrow_to_buffer
 * Description: Convert arrow table into record batches which are dumped on to a
 *              output buffer. For converting arrow table three things are essential.
 *  a. Where to write - In this case we are using buffer, but it can be streams or
 *                      files as well
 *  b. OutputStream - We connect a buffer (or file) to the stream and writer writes
 *                    data from this stream. We are using arrow::BufferOutputStream
 *                    as an output stream.
 *  c. Writer - Does writing data to OutputStream. We are using arrow::RecordBatchStreamWriter
 *              which will write the data to output stream.
 * @param[in] table   : Arrow table to be converted
 * @param[out] buffer : Output buffer
 * Return Value: error code
 */
static
int convert_arrow_to_buffer(const std::shared_ptr<arrow::Table> &table, std::shared_ptr<arrow::Buffer>* buffer)
{
    CLS_LOG(20, "Writing arrow to a buffer");

    // Initilization related to writing to the the file
    std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
    std::shared_ptr<arrow::io::BufferOutputStream> out;
    arrow::io::BufferOutputStream::Create(STREAM_CAPACITY, arrow::default_memory_pool(), &out);
    arrow::io::OutputStream *raw_out = out.get();
    arrow::Table *raw_table = table.get();
    arrow::ipc::RecordBatchStreamWriter::Open(raw_out, raw_table->schema(), &writer);

    // Initilization related to reading from arrow
    writer->WriteTable(*(table.get()));
    writer->Close();
    out->Finish(buffer);
    return 0;
}

/*
 * Function: compress_arrow_tables
 * Description: Compress the given arrow tables into single arrow table. Before
 * compression check if schema for all that tables are same.
 * @param[in] table_vec : Vector of tables to be compressed
 * @param[out] table    : Output arrow table
 * Return Value: error code
 */
int compress_arrow_tables(std::vector<std::shared_ptr<arrow::Table>> &table_vec,
                          std::shared_ptr<arrow::Table> *table)
{
    auto table_ptr = *table_vec.begin();
    auto original_schema = (table_ptr)->schema();

    // Check if schema for all tables are same, otherwise return error
    for (auto it = table_vec.begin(); it != table_vec.end(); it++) {
        auto table_schema = (*it)->schema();
        if (!original_schema->Equals(*table_schema.get())) {
            CLS_ERR("ERROR: compress_arrow_tables: Schema for tables are not same");
            return -EINVAL;
        }
    }

    // TODO: Change schema metadata for the created table
    RETURN_ON_FAILURE(arrow::ConcatenateTables(table_vec, table));
    return 0;
}

/*
 * Function: split_arrow_table
 * Description: Split the given arrow table into number of arrow tables. Based on
 * input parameter (max_rows) spliting is done.
 * @param[in] table      : Table to be split.
 * @param[out] max_rows  : Maximum number of rows a table can have.
 * @param[out] table_vec : Vector of tables created after split.
 * Return Value: error code
 */
int split_arrow_table(std::shared_ptr<arrow::Table> &table, int max_rows,
                      std::vector<std::shared_ptr<arrow::Table>>* table_vec)
{
    auto orig_schema = table->schema();
    auto orig_metadata = orig_schema->metadata();
    int orig_num_cols = table->num_columns();
    int remaining_rows = std::stoi(orig_metadata->value(METADATA_NUM_ROWS));
    int offset = 0;

    while ((remaining_rows / max_rows) >= 1) {

        // Extract skyhook metadata from original table.
        std::shared_ptr<arrow::KeyValueMetadata> metadata (new arrow::KeyValueMetadata);
        metadata->Append(ToString(METADATA_SKYHOOK_VERSION),
                         orig_metadata->value(METADATA_SKYHOOK_VERSION));
        metadata->Append(ToString(METADATA_DATA_SCHEMA_VERSION),
                         orig_metadata->value(METADATA_DATA_SCHEMA_VERSION));
        metadata->Append(ToString(METADATA_DATA_STRUCTURE_VERSION),
                         orig_metadata->value(METADATA_DATA_STRUCTURE_VERSION));
        metadata->Append(ToString(METADATA_DATA_FORMAT_TYPE),
                         orig_metadata->value(METADATA_DATA_FORMAT_TYPE));
        metadata->Append(ToString(METADATA_DATA_SCHEMA),
                         orig_metadata->value(METADATA_DATA_SCHEMA));
        metadata->Append(ToString(METADATA_DB_SCHEMA),
                         orig_metadata->value(METADATA_DB_SCHEMA));
        metadata->Append(ToString(METADATA_TABLE_NAME),
                         orig_metadata->value(METADATA_TABLE_NAME));
        
        if (remaining_rows <= max_rows)
            metadata->Append(ToString(METADATA_NUM_ROWS),
                             std::to_string(remaining_rows));
        else
            metadata->Append(ToString(METADATA_NUM_ROWS),
                             std::to_string(max_rows));

        // Generate the schema for new table using original table schema
        auto schema = std::make_shared<arrow::Schema>(orig_schema->fields(), metadata);

        // Split and create the columns from original table
        std::vector<std::shared_ptr<arrow::Column>> column_list;
        for (int i = 0; i < orig_num_cols; i++) {
            std::shared_ptr<arrow::Column> column;
            if (remaining_rows <= max_rows)
                column = table->column(i)->Slice(offset);
            else
                column = table->column(i)->Slice(offset, max_rows);
            column_list.emplace_back(column);
        }
        offset += max_rows;

        // Finally, create the arrow table based on schema and column vector
        std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, column_list);
        table_vec->push_back(table);
        remaining_rows -= max_rows;
    }
    return 0;
}

/*
 * Function: transform_fb_to_arrow
 * Description: Build arrow schema vector using skyhook schema information. Get the
 *              details of columns from skyhook schema and using array builders for
 *              each datatype add the data to array vectors. Finally, create arrow
 *              table using array vectors and schema vector.
 * @param[in] fb      : Flatbuffer to be converted
 * @param[in] size    : Size of the flatbuffer
 * @param[out] errmsg : errmsg buffer
 * @param[out] buffer : arrow table
 * Return Value: error code
 */
static
int transform_fb_to_arrow(const char* fb,
                          const size_t fb_size,
                          std::string& errmsg,
                          std::shared_ptr<arrow::Table>* table)
{
    int errcode = 0;
    sky_root root = getSkyRoot(fb, fb_size);
    schema_vec sc = schemaFromString(root.data_schema);
    delete_vector del_vec = root.delete_vec;
    uint32_t nrows = root.nrows;

    // Initialization related to Apache Arrow
    auto pool = arrow::default_memory_pool();
    std::vector<arrow::ArrayBuilder *> builder_list;
    std::vector<std::shared_ptr<arrow::Array>> array_list;
    std::vector<std::shared_ptr<arrow::Field>> schema_vector;
    std::shared_ptr<arrow::KeyValueMetadata> metadata (new arrow::KeyValueMetadata);

    // Add skyhook metadata to arrow metadata.
    metadata->Append(ToString(METADATA_SKYHOOK_VERSION),
                     std::to_string(root.skyhook_version));
    metadata->Append(ToString(METADATA_DATA_SCHEMA_VERSION),
                     std::to_string(root.data_schema_version));
    metadata->Append(ToString(METADATA_DATA_STRUCTURE_VERSION),
                     std::to_string(root.data_structure_version));
    metadata->Append(ToString(METADATA_DATA_FORMAT_TYPE),
                     std::to_string(root.data_format_type));
    metadata->Append(ToString(METADATA_DATA_SCHEMA), root.data_schema);
    metadata->Append(ToString(METADATA_DB_SCHEMA), root.db_schema);
    metadata->Append(ToString(METADATA_TABLE_NAME), root.table_name);
    metadata->Append(ToString(METADATA_NUM_ROWS), std::to_string(root.nrows));

    // Iterate through schema vector to get the details of columns i.e name and type.
    for (auto it = sc.begin(); it != sc.end() && !errcode; ++it) {
        col_info col = *it;

        // Create the array builders for respective datatypes. Use these array
        // builders to store data to array vectors. These array vectors holds the
        // actual column values. Also, add the details of column

        switch(col.type) {

            case SDT_BOOL: {
                auto ptr = std::unique_ptr<arrow::ArrayBuilder>(new arrow::BooleanBuilder(pool));
                builder_list.emplace_back(ptr.get());
                ptr.release();
                schema_vector.push_back(arrow::field(col.name, arrow::boolean()));
                break;
            }
            case SDT_INT8: {
                auto ptr = std::unique_ptr<arrow::ArrayBuilder>(new arrow::Int8Builder(pool));
                builder_list.emplace_back(ptr.get());
                ptr.release();
                schema_vector.push_back(arrow::field(col.name, arrow::int8()));
                break;
            }
            case SDT_INT16: {
                auto ptr = std::unique_ptr<arrow::ArrayBuilder>(new arrow::Int16Builder(pool));
                builder_list.emplace_back(ptr.get());
                ptr.release();
                schema_vector.push_back(arrow::field(col.name, arrow::int16()));
                break;
            }
            case SDT_INT32: {
                auto ptr = std::unique_ptr<arrow::ArrayBuilder>(new arrow::Int32Builder(pool));
                builder_list.emplace_back(ptr.get());
                ptr.release();
                schema_vector.push_back(arrow::field(col.name, arrow::int32()));
                break;
            }
            case SDT_INT64: {
                auto ptr = std::unique_ptr<arrow::ArrayBuilder>(new arrow::Int64Builder(pool));
                builder_list.emplace_back(ptr.get());
                ptr.release();
                schema_vector.push_back(arrow::field(col.name, arrow::int64()));
                break;
            }
            case SDT_UINT8: {
                auto ptr = std::unique_ptr<arrow::ArrayBuilder>(new arrow::UInt8Builder(pool));
                builder_list.emplace_back(ptr.get());
                ptr.release();
                schema_vector.push_back(arrow::field(col.name, arrow::uint8()));
                break;
            }
            case SDT_UINT16: {
                auto ptr = std::unique_ptr<arrow::ArrayBuilder>(new arrow::UInt16Builder(pool));
                builder_list.emplace_back(ptr.get());
                ptr.release();
                schema_vector.push_back(arrow::field(col.name, arrow::uint16()));
                break;
            }
            case SDT_UINT32: {
                auto ptr = std::unique_ptr<arrow::ArrayBuilder>(new arrow::UInt32Builder(pool));
                builder_list.emplace_back(ptr.get());
                ptr.release();
                schema_vector.push_back(arrow::field(col.name, arrow::uint32()));
                break;
            }
            case SDT_UINT64: {
                auto ptr = std::unique_ptr<arrow::ArrayBuilder>(new arrow::UInt64Builder(pool));
                builder_list.emplace_back(ptr.get());
                ptr.release();
                schema_vector.push_back(arrow::field(col.name, arrow::uint64()));
                break;
            }
            case SDT_FLOAT: {
                auto ptr = std::unique_ptr<arrow::ArrayBuilder>(new arrow::FloatBuilder(pool));
                builder_list.emplace_back(ptr.get());
                ptr.release();
                schema_vector.push_back(arrow::field(col.name, arrow::float32()));
                break;
            }
            case SDT_DOUBLE: {
                auto ptr = std::unique_ptr<arrow::ArrayBuilder>(new arrow::DoubleBuilder(pool));
                builder_list.emplace_back(ptr.get());
                ptr.release();
                schema_vector.push_back(arrow::field(col.name, arrow::float64()));
                break;
            }
            case SDT_CHAR: {
                auto ptr = std::unique_ptr<arrow::ArrayBuilder>(new arrow::Int8Builder(pool));
                builder_list.emplace_back(ptr.get());
                ptr.release();
                schema_vector.push_back(arrow::field(col.name, arrow::int8()));
                break;
            }
            case SDT_UCHAR: {
                auto ptr = std::unique_ptr<arrow::ArrayBuilder>(new arrow::UInt8Builder(pool));
                builder_list.emplace_back(ptr.get());
                ptr.release();
                schema_vector.push_back(arrow::field(col.name, arrow::uint8()));
                break;
            }
            case SDT_DATE:
            case SDT_STRING: {
                auto ptr = std::unique_ptr<arrow::ArrayBuilder>(new arrow::StringBuilder(pool));
                builder_list.emplace_back(ptr.get());
                ptr.release();
                schema_vector.push_back(arrow::field(col.name, arrow::utf8()));
                break;
            }
            default: {
                errcode = TablesErrCodes::UnsupportedSkyDataType;
                errmsg.append("ERROR transform_row_to_col(): table=" +
                              root.table_name + " col.type=" +
                              std::to_string(col.type) +
                              " UnsupportedSkyDataType.");
                return errcode;
            }
        }
    }

    // Add RID column
    auto rid_ptr = std::unique_ptr<arrow::ArrayBuilder>(new arrow::Int64Builder(pool));
    builder_list.emplace_back(rid_ptr.get());
    rid_ptr.release();
    schema_vector.push_back(arrow::field("RID", arrow::int64()));

    // Add deleted vector column
    auto dv_ptr = std::unique_ptr<arrow::ArrayBuilder>(new arrow::BooleanBuilder(pool));
    builder_list.emplace_back(dv_ptr.get());
    dv_ptr.release();
    schema_vector.push_back(arrow::field("DELETED_VECTOR", arrow::boolean()));

    // Iterate through rows and store data in each row in respective columns.
    for (uint32_t i = 0; i < nrows; i++) {

        // Get a skyhook record struct
        sky_rec rec = getSkyRec(root.offs->Get(i));

        // Get the row as a vector.
        auto row = rec.data.AsVector();

        // For the current row, go from 0 to num_cols and append the data into array
        // builders.
        for (auto it = sc.begin(); it != sc.end() && !errcode; ++it) {
            auto builder = builder_list[std::distance(sc.begin(), it)];
            col_info col = *it;
            // Append data to the respective data type builders
            switch(col.type) {

                case SDT_BOOL:
                    static_cast<arrow::BooleanBuilder *>(builder)->Append(row[col.idx].AsBool());
                    break;
                case SDT_INT8:
                    static_cast<arrow::Int8Builder *>(builder)->Append(row[col.idx].AsInt8());
                    break;
                case SDT_INT16:
                    static_cast<arrow::Int16Builder *>(builder)->Append(row[col.idx].AsInt16());
                    break;
                case SDT_INT32:
                    static_cast<arrow::Int32Builder *>(builder)->Append(row[col.idx].AsInt32());
                    break;
                case SDT_INT64:
                    static_cast<arrow::Int64Builder *>(builder)->Append(row[col.idx].AsInt64());
                    break;
                case SDT_UINT8:
                    static_cast<arrow::UInt8Builder *>(builder)->Append(row[col.idx].AsUInt8());
                    break;
                case SDT_UINT16:
                    static_cast<arrow::UInt16Builder *>(builder)->Append(row[col.idx].AsUInt16());
                    break;
                case SDT_UINT32:
                    static_cast<arrow::UInt32Builder *>(builder)->Append(row[col.idx].AsUInt32());
                    break;
                case SDT_UINT64:
                    static_cast<arrow::UInt64Builder *>(builder)->Append(row[col.idx].AsUInt64());
                    break;
                case SDT_FLOAT:
                    static_cast<arrow::FloatBuilder *>(builder)->Append(row[col.idx].AsFloat());
                    break;
                case SDT_DOUBLE:
                    static_cast<arrow::DoubleBuilder *>(builder)->Append(row[col.idx].AsDouble());
                    break;
                case SDT_CHAR:
                    static_cast<arrow::Int8Builder *>(builder)->Append(row[col.idx].AsInt8());
                    break;
                case SDT_UCHAR:
                    static_cast<arrow::UInt8Builder *>(builder)->Append(row[col.idx].AsUInt8());
                    break;
                case SDT_DATE:
                case SDT_STRING:
                    static_cast<arrow::StringBuilder *>(builder)->Append(row[col.idx].AsString().str());
                    break;
                default: {
                    errcode = TablesErrCodes::UnsupportedSkyDataType;
                    errmsg.append("ERROR transform_row_to_col(): table=" +
                                  root.table_name + " col.type=" +
                                  std::to_string(col.type) +
                                  " UnsupportedSkyDataType.");
                }
            }
        }

        // Add entries for RID and Deleted vector
        int num_cols = std::distance(sc.begin(), sc.end());
        static_cast<arrow::Int64Builder *>(builder_list[ARROW_RID_INDEX(num_cols)])->Append(rec.RID);
        static_cast<arrow::UInt8Builder *>(builder_list[ARROW_DELVEC_INDEX(num_cols)])->Append(del_vec[i]);

    }

    // Finalize the arrays holding the data
    for (auto it = builder_list.begin(); it != builder_list.end(); ++it) {
        auto builder = *it;
        std::shared_ptr<arrow::Array> array;
        builder->Finish(&array);
        array_list.push_back(array);
        delete builder;
    }

    // Generate schema from schema vector and add the metadata
    auto schema = std::make_shared<arrow::Schema>(schema_vector, metadata);

    // Finally, create a arrow table from schema and array vector
    *table = arrow::Table::Make(schema, array_list);

    return errcode;
}

static
int transform_arrow_to_fb(const char* data,
                          const size_t data_size,
                          std::string& errmsg,
                          flatbuffers::FlatBufferBuilder& flatbldr)
{
    int ret;

    // Placeholder function
    std::shared_ptr<arrow::Table> table;
    std::shared_ptr<arrow::Buffer> buffer;

    std::string str_data(data, data_size);
    arrow::Buffer::FromString(str_data, &buffer);

    extract_arrow_from_buffer(&table, buffer);

    ret = print_arrowbuf_colwise(table);
    if (ret != 0) {
        CLS_ERR("ERROR: Printing object");
        return ret;
    }
    return 0;
}

//TODO: This is a test code for checking decode arrow table works fine or not
int test_bls(bufferlist *wrapped_bls)
{
    // Create bl1
    bufferlist bl1;
    std::shared_ptr<arrow::Table> table1;
    std::shared_ptr<arrow::Buffer> buffer1;
    read_from_file("/tmp/skyhook_1.arrow", &buffer1);
    bl1.append(buffer1->ToString().c_str(), buffer1->size());
    ::encode(bl1, *wrapped_bls);

    // Create bl2
    bufferlist bl2;
    std::shared_ptr<arrow::Table> table2;
    std::shared_ptr<arrow::Buffer> buffer2;
    read_from_file("/tmp/skyhook_2.arrow", &buffer2);
    bl2.append(buffer2->ToString().c_str(), buffer2->size());
    ::encode(bl2, *wrapped_bls);
    return 0;
}

/*
 * Function: transform_db_op
 * Description: Method to convert database layout.
 * @param[in] hctx    : CLS method context
 * @param[out] in     : input bufferlist
 * @param[out] out    : output bufferlist
 * Return Value: error code
 */
static
int transform_db_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    int layout_type = 0;
    transform_op op;

    // unpack the requested op from the inbl.
    try {
        bufferlist::iterator it = in->begin();
        ::decode(op, it);
    } catch (const buffer::error &err) {
        CLS_ERR("ERROR: cls_tabular:transform_db_op: decoding transform_op");
        return -EINVAL;
    }

    CLS_LOG(20, "transform_db_op: table_name=%s", op.table_name.c_str());
    CLS_LOG(20, "transform_db_op: data_schema=%s", op.data_schema.c_str());
    CLS_LOG(20, "transform_db_op: transform_layout_type=%d", op.required_type);

    int ret = get_sky_layout_type(hctx, layout_type);
    if (ret == -ENOENT || ret == -ENODATA) {
        // If sky_layout_type is not present then insert it in xattr.
        // Default value is set as a Flatbuffer
        ret = set_sky_layout_type(hctx, SFT_FLATBUF_FLEX_ROW);
        if(ret < 0) {
            CLS_ERR("transform_db_op: error setting sky_layout_type entry to xattr %d", ret);
            return ret;
        }
        layout_type = SFT_FLATBUF_FLEX_ROW;
    }
    else if (ret < 0) {
        CLS_ERR("ERROR: transform_db_op: sky_layout_type entry from xattr %d", ret);
        return ret;
    }

    // Check if transformation is required or not
    if (layout_type == op.required_type) {
        // Source and destination object types are same, therefore no tranformation
        // is required.
        CLS_LOG(20, "No Transforming required");
        return 0;
    }

    // Object contains one bl that itself wraps a seq of encoded bls of skyhook fb/arrow
    bufferlist wrapped_bls;
    ret = cls_cxx_read(hctx, 0, 0, &wrapped_bls);
    if (ret < 0) {
        CLS_ERR("ERROR: transform_db_op: reading obj. %d", ret);
        return ret;
    }

    using namespace Tables;
    ceph::bufferlist::iterator it = wrapped_bls.begin();
    while (it.get_remaining() > 0) {
        bufferlist bl;
        try {
            ::decode(bl, it);  // unpack the next bl
        } catch (const buffer::error &err) {
            CLS_ERR("ERROR: decoding object format from BL");
            return -EINVAL;
        }

        // Get our data as contiguous bytes
        const char* data = bl.c_str();
        size_t data_size = bl.length();
        std::string errmsg;

        if (op.required_type == SFT_ARROW) {
            std::shared_ptr<arrow::Table> table;
            ret = transform_fb_to_arrow(data, data_size, errmsg, &table);
            if (ret != 0) {
                CLS_ERR("ERROR: transforming object from flatbuffer to arrow");
                return ret;
            }

            ret = print_arrowbuf_rowwise(table);
            if (ret != 0) {
                CLS_ERR("ERROR: Printing arrow object rowwise");
                return ret;
            }

            // Convert arrow to a buffer
            std::shared_ptr<arrow::Buffer> buffer;
            convert_arrow_to_buffer(table, &buffer);
            write_to_file("/tmp/skyhook.arrow", buffer.get());

            // Extract arrow from the buffer
            std::shared_ptr<arrow::Table> read_table;
            std::shared_ptr<arrow::Buffer> read_buffer;
            read_from_file("/tmp/skyhook.arrow", &read_buffer);
            extract_arrow_from_buffer(&read_table, read_buffer);

            ret = print_arrowbuf_colwise(read_table);
            if (ret != 0) {
                CLS_ERR("ERROR: Printing object");
                return ret;
            }
        } else if (op.required_type == SFT_FLATBUF_FLEX_ROW) {
            flatbuffers::FlatBufferBuilder flatbldr(1024);  // pre-alloc sz

            ret = transform_arrow_to_fb(data, data_size, errmsg, flatbldr);
            if (ret != 0) {
                CLS_ERR("ERROR: transforming object from arrow to flatbuffer");
                return ret;
            }
        }
    }

    // Set the destination object type
    ret = set_sky_layout_type(hctx, op.required_type);
    if(ret < 0) {
        CLS_ERR("transform_db_op: error setting sky_layout_type entry to xattr %d", ret);
        return ret;
    }
    return 0;
}


void __cls_init()
{
  CLS_LOG(20, "Loaded tabular class!");

  cls_register("tabular", &h_class);

  cls_register_cxx_method(h_class, "exec_query_op",
      CLS_METHOD_RD, exec_query_op, &h_exec_query_op);

  cls_register_cxx_method(h_class, "exec_runstats_op",
      CLS_METHOD_RD | CLS_METHOD_WR, exec_runstats_op, &h_exec_runstats_op);

  cls_register_cxx_method(h_class, "build_index",
      CLS_METHOD_RD | CLS_METHOD_WR, build_index, &h_build_index);

  cls_register_cxx_method(h_class, "exec_build_sky_index_op",
      CLS_METHOD_RD | CLS_METHOD_WR, exec_build_sky_index_op, &h_exec_build_sky_index_op);

  cls_register_cxx_method(h_class, "transform_db_op",
      CLS_METHOD_RD | CLS_METHOD_WR, transform_db_op, &h_transform_db_op);


}

