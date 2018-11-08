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
#include "cls_tabular.h"
#include "cls_tabular_utils.h"

CLS_VER(1,0)
CLS_NAME(tabular)

cls_handle_t h_class;
cls_method_handle_t h_query_op;
cls_method_handle_t h_build_index;
cls_method_handle_t h_build_sky_index;


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



static
int build_sky_index(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    // iterate over all fbs within an obj and create 2 indexes:
    // 1. for each fb, create an omap entry with its fb_num and byte off (phys)
    // 2. for each row of an fb, create a rec entry for the keycol(s) (logical)
    // value and fb_num

    const int ceph_bl_encoding_len = 4; // ?? seems to be an int
    int fb_cnt = 0;

    // points (physically) to the fb containing the row
    // <"oid-fbseqnum",<idx_fb_entry>> i.e., <str_oid_fbseqnum, <off, len>>
    std::map<std::string, bufferlist> fbs_index;

    // points (logically) to the relevant row within the fb
    // <"col(s)_val",<idx_val_entry>> i.e., <col_val, map::<fb_num, row_num>>
    std::map<std::string, bufferlist> recs_index;

    // extract the index op instructions from the input bl
    idx_op op;
    try {
        bufferlist::iterator it = in->begin();
        ::decode(op, it);
    } catch (const buffer::error &err) {
        CLS_ERR("ERROR: cls_tabular:build_sky_idx: decoding idx_op");
        return -EINVAL;
    }
    Tables::schema_vec idx_schema;
    Tables::schemaFromString(idx_schema, op.idx_schema_str);

    // obj contains one bl that itself wraps a seq of encoded bls of skyhook fb
    bufferlist wrapped_bls;
    int ret = cls_cxx_read(hctx, 0, 0, &wrapped_bls);
    if (ret < 0) {
        CLS_ERR("ERROR: cls_tabular:build_sky_index: reading obj %d", ret);
        return ret;
    }

    // decode and process each bl (contains 1 flatbuf) in a loop.
    uint64_t off = 0;
    ceph::bufferlist::iterator it = wrapped_bls.begin();
    uint64_t obj_len = it.get_remaining();
    while (it.get_remaining() > 0) {
        off = obj_len - it.get_remaining();
        ceph::bufferlist bl;
        try {
            ::decode(bl, it);  // unpack the next bl (flatbuf)
        } catch (ceph::buffer::error&) {
            assert(Tables::BuildSkyIndexDecodeBlsErr==0);
        }

        // get our data as contiguous bytes before accessing as flatbuf
        const char* fb = bl.c_str();
        int fb_len = bl.length();
        Tables::sky_root root = Tables::getSkyRoot(fb, fb_len);

        // create an idx_fb_entry for this fb's phys loc info.
        // get current max fb_seq_num;
        uint64_t fb_seq_num = ++fb_cnt;  // TODO: should be root.fb_seq_num;
        struct idx_fb_entry fb_ent(off, fb_len + ceph_bl_encoding_len);
        bufferlist fb_bl;
        ::encode(fb_ent, fb_bl);
        std::string fbkey = Tables::u64tostr(fb_seq_num);
        int len = fbkey.length();
        fbkey = fbkey.substr(len-10, len);
        fbs_index[fbkey] = fb_bl;
        //CLS_LOG(20,"fb-key=%s", (fbkey+"; "+fb_ent.toString()).c_str());

        // create a idx_rec_entry for the keycols of each row
        for (uint32_t i = 0; i < root.nrows; i++) {

            Tables::sky_rec rec = Tables::getSkyRec(root.offs->Get(i));
            auto row = rec.data.AsVector();

            // create the index key from the schema cols
            std::string strkey;
            for (auto it = idx_schema.begin(); it != idx_schema.end(); ++it) {
                int ret = Tables::buildStrKey(row[(*it).idx].AsUInt64(),
                                              (*it).type, strkey);
                if (ret) {
                    CLS_ERR("error buildStrKey for RID %ld entry," \
                            "TablesErrCodes::%d", rec.RID, ret);
                    return -1;
                }
            }

            if (strkey.empty()) {
                CLS_ERR("error key for RID %ld entry," \
                        " TablesErrCodes::%d", rec.RID,
                        Tables::BuildSkyIndexKeyCreationFailed);
                return -1;
            }

            // create the index val
            struct idx_rec_entry rec_ent(fb_seq_num, i, rec.RID);
            bufferlist rec_bl;
            ::encode(rec_ent, rec_bl);
            recs_index[strkey] = rec_bl;  // TODO: verify if non-unique key
            //CLS_LOG(20,"rec-key=%s",(strkey+";"+rec_ent.toString()).c_str());

            // add keys in batches to minimize IOs
            if (recs_index.size() > op.batch_size) {
                ret = cls_cxx_map_set_vals(hctx, &recs_index);
                if (ret < 0) {
                    CLS_ERR("error setting index entries %d", ret);
                    return ret;
                }
                recs_index.clear();
            }
        }  // end foreach row
    }  // end while decode wrapped_bls

    // always add remaining recs to recs_index and fbs to fbs_index
    ret = cls_cxx_map_set_vals(hctx, &fbs_index);
    if (ret < 0) {
        CLS_ERR("error setting index fbs entries %d", ret);
        return ret;
    }
    if (recs_index.size() > 0) {
        ret = cls_cxx_map_set_vals(hctx, &recs_index);
        if (ret < 0) {
            CLS_ERR("error setting index recs entries %d", ret);
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

/*
 * Primary method to process our test queries qa--qf
 */
static int query_op_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  query_op op;
  // extract the query op to get the query request params
  try {
    bufferlist::iterator it = in->begin();
    ::decode(op, it);
  } catch (const buffer::error &err) {
    CLS_ERR("ERROR: decoding query op");
    return -EINVAL;
  }

  // represents total rows considered during processing, including those
  // skipped by using an index or other filtering, this should match the
  // total rows stored or indexed in the object.
  uint64_t rows_processed = 0;

  // track the bufferlist read time (read_ns) vs. the processing time (eval_ns)
  uint64_t read_ns = 0;
  uint64_t eval_ns_start = 0;

  bufferlist result_bl;  // result set to be returned to client.

    if (op.query == "flatbuf") {

        bufferlist b;
        uint64_t start = getns();
        int ret = cls_cxx_read(hctx, 0, 0, &b);  // read entire object.
        if (ret < 0) {
          CLS_ERR("ERROR: reading flatbuf obj %d", ret);
          return ret;
        }
        read_ns = getns() - start;
        eval_ns_start = getns();

        if (op.fastpath == true) {
            result_bl = b;
            // note fastpath will not increment any rows_processed
        } else {
            // get the inbound (current schema) and outbound (query) schema,
            // once per obj, before we start unpacking the fbs.

            // schema_in is the table's current schema
            Tables::schema_vec schema_in;
            Tables::schemaFromString(schema_in, op.table_schema_str);

            // schema_out is the query schema
            Tables::schema_vec schema_out;
            Tables::schemaFromString(schema_out, op.query_schema_str);

            // predicates to be applied, if any
            Tables::predicate_vec preds;
            Tables::predsFromString(preds, schema_in, op.predicate_str);

            // decode and process each bl (contains 1 flatbuf) in a loop.
            ceph::bufferlist::iterator it = b.begin();
            while (it.get_remaining() > 0) {
                bufferlist bl;
                try {
                    ::decode(bl, it);  // unpack the next bl (flatbuf)
                } catch (const buffer::error &err) {
                    CLS_ERR("ERROR: decoding flatbuf from BL");
                    return -EINVAL;
                }

                // get our data as contiguous bytes before accessing as flatbuf
                const char* fb = bl.c_str();
                size_t fb_size = bl.length();

                Tables::sky_root root = Tables::getSkyRoot(fb, fb_size);
                flatbuffers::FlatBufferBuilder flatbldr(1024);  // pre-alloc sz
                std::string errmsg;
                ret = Tables::processSkyFb(flatbldr, schema_in, schema_out,
                                           preds, fb, fb_size, errmsg);
                if (ret != 0) {
                    CLS_ERR("ERROR: processing flatbuf, %s", errmsg.c_str());
                    CLS_ERR("ERROR: TablesErrCodes::%d", ret);
                    return -1;
                }
                rows_processed += root.nrows;
                const char *processed_fb = \
                    reinterpret_cast<char*>(flatbldr.GetBufferPointer());
                int bufsz = flatbldr.GetSize();

                // add this processed fb to our sequence of bls
                bufferlist ans;
                ans.append(processed_fb, bufsz);
                ::encode(ans, result_bl);
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

      eval_ns_start = getns();

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
          const char *row = rows + rid * row_size;  // offset to row
          const char *vptr = row + extended_price_field_offset;  // offset to col
          const double val = *(const double*)vptr;  // extract data as col type
          if (val > op.extended_price) {  // apply filter
            result_count++;  // counter of matching rows for this count(*) query
            // when a predicate passes, add some extra work
            add_extra_row_cost(op.extra_row_cost);
          }
        }
        ::encode(result_count, result_bl);  // store count into our result set.
      } else if (op.query == "b") {  // range query on extended_price col
        if (op.projection) {  // only add (orderkey,linenum) col data to result set
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
      } else if (op.query == "d") {// point query on primary key (orderkey,linenum)
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
  }

  uint64_t eval_ns = getns() - eval_ns_start;

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
