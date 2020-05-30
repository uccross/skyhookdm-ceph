/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#ifndef CLS_TABULAR_H
#define CLS_TABULAR_H

#include <include/types.h>


void cls_log_message(std::string msg, bool is_err, int log_level);


// used by Arrow format only
#define STREAM_CAPACITY 1024
#define ARROW_RID_INDEX(cols) (cols)
#define ARROW_DELVEC_INDEX(cols) (cols + 1)

// metadata corresponding to struct sky_root table
enum arrow_metadata_t {
    METADATA_SKYHOOK_VERSION = 0,
    METADATA_DATA_SCHEMA_VERSION,
    METADATA_DATA_STRUCTURE_VERSION,
    METADATA_DATA_FORMAT_TYPE,
    METADATA_DATA_SCHEMA,
    METADATA_DB_SCHEMA,
    METADATA_TABLE_NAME,
    METADATA_NUM_ROWS
};

enum pyarrow_metadata_t {
    PYARROW_METADATA_DATA_SCHEMA = 0,
};

inline const char* ToString(pyarrow_metadata_t m)
{
    switch (m)
    {
        case PYARROW_METADATA_DATA_SCHEMA:      return "data_schema";
        default:                                return "[Unknown Metadata]";
    }
}

inline const char* ToString(arrow_metadata_t m)
{
    switch (m)
    {
        case METADATA_SKYHOOK_VERSION:         return "skyhook_version";
        case METADATA_DATA_SCHEMA_VERSION:     return "data_schema_version";
        case METADATA_DATA_STRUCTURE_VERSION:  return "data_structure_version";
        case METADATA_DATA_FORMAT_TYPE:        return "data_format_type";
        case METADATA_DATA_SCHEMA:             return "data_schema";
        case METADATA_DB_SCHEMA:               return "db_schema";
        case METADATA_TABLE_NAME:              return "table_name";
        case METADATA_NUM_ROWS:                return "num_rows";
        default:                               return "[Unknown Metadata]";
    }
}

// refers to data format stored in objects
enum SkyFormatType {
    SFT_FLATBUF_FLEX_ROW = 1,
    SFT_FLATBUF_UNION_ROW,
    SFT_FLATBUF_UNION_COL,
    SFT_FLATBUF_CSV_ROW,
    SFT_ARROW,
    SFT_PARQUET,
    SFT_PG_TUPLE,
    SFT_CSV,
    SFT_JSON,
    SFT_PG_BINARY,
    SFT_PYARROW_BINARY,
    SFT_HDF5,
    SFT_EXAMPLE_FORMAT,
    SFT_ANY
};


inline int sky_format_type_from_string (std::string type) {
    std::transform(type.begin(), type.end(), type.begin(), ::toupper);
    if (type == "SFT_FLATBUF_FLEX_ROW")  return SFT_FLATBUF_FLEX_ROW;
    if (type == "SFT_FLATBUF_UNION_ROW") return SFT_FLATBUF_UNION_ROW;
    if (type == "SFT_FLATBUF_UNION_COL") return SFT_FLATBUF_UNION_COL;
    if (type == "SFT_FLATBUF_CSV_ROW")   return SFT_FLATBUF_CSV_ROW;
    if (type == "SFT_ARROW")             return SFT_ARROW;
    if (type == "SFT_PARQUET")           return SFT_PARQUET;
    if (type == "SFT_PG_TUPLE")          return SFT_PG_TUPLE;
    if (type == "SFT_CSV")               return SFT_CSV;
    if (type == "SFT_PG_BINARY")         return SFT_PG_BINARY;
    if (type == "SFT_PYARROW_BINARY")    return SFT_PYARROW_BINARY;
    if (type == "SFT_HDF5")              return SFT_HDF5;
    if (type == "SFT_JSON")              return SFT_JSON;
    if (type == "SFT_EXAMPLE_FORMAT")    return SFT_EXAMPLE_FORMAT;
    return 0;   // format unrecognized
}

enum CompressionType {
    none = 0,
    // lzw,  TODO: placeholder, not yet supported.
    // bz2,
    // etc.
};

/*
 * Stores the query request parameters.  This is encoded by the client and
 * decoded by server (osd node) for query processing.
 */
struct query_op {

  // query parameters
  std::string query;   // query type TODO: remove
  bool debug;
  bool fastpath;
  bool index_read;
  bool mem_constrain;
  int index_type;
  int index2_type;
  int index_plan_type;
  int index_batch_size;
  int result_format;  // SkyFormatType enum
  std::string db_schema_name;
  std::string table_name;
  std::string data_schema;
  std::string query_schema;
  std::string index_schema;
  std::string index2_schema;
  std::string query_preds;
  std::string index_preds;
  std::string index2_preds;

  query_op() {}

  // serialize the fields into bufferlist to be sent over the wire
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(debug, bl);
    ::encode(query, bl);
    ::encode(fastpath, bl);
    ::encode(index_read, bl);
    ::encode(mem_constrain, bl);
    ::encode(index_type, bl);
    ::encode(index2_type, bl);
    ::encode(index_plan_type, bl);
    ::encode(index_batch_size, bl);
    ::encode(result_format, bl);
    ::encode(db_schema_name, bl);
    ::encode(table_name, bl);
    ::encode(data_schema, bl);
    ::encode(query_schema, bl);
    ::encode(index_schema, bl);
    ::encode(index2_schema, bl);
    ::encode(query_preds, bl);
    ::encode(index_preds, bl);
    ::encode(index2_preds, bl);
    ENCODE_FINISH(bl);
  }

  // deserialize the fields from the bufferlist into this struct
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(debug, bl);
    ::decode(query, bl);
    ::decode(fastpath, bl);
    ::decode(index_read, bl);
    ::decode(mem_constrain, bl);
    ::decode(index_type, bl);
    ::decode(index2_type, bl);
    ::decode(index_plan_type, bl);
    ::decode(index_batch_size, bl);
    ::decode(result_format, bl);
    ::decode(db_schema_name, bl);
    ::decode(table_name, bl);
    ::decode(data_schema, bl);
    ::decode(query_schema, bl);
    ::decode(index_schema, bl);
    ::decode(index2_schema, bl);
    ::decode(query_preds, bl);
    ::decode(index_preds, bl);
    ::decode(index2_preds, bl);
    DECODE_FINISH(bl);
  }

  std::string toString() {
    std::string s;
    s.append("query_op:");
    s.append(" .debug=" + std::to_string(debug));
    s.append(" .fastpath=" + std::to_string(fastpath));
    s.append(" .index_read=" + std::to_string(index_read));
    s.append(" .index_type=" + std::to_string(index_type));
    s.append(" .index2_type=" + std::to_string(index2_type));
    s.append(" .index_plan_type=" + std::to_string(index_plan_type));
    s.append(" .index_batch_size=" + std::to_string(index_batch_size));
    s.append(" .result_format=" + std::to_string(result_format));
    s.append(" .db_schema_name=" + db_schema_name);
    s.append(" .table_name=" + table_name);
    s.append(" .data_schema=" + data_schema);
    s.append(" .query_schema=" + query_schema);
    s.append(" .index_schema=" + index_schema);
    s.append(" .index2_schema=" + index2_schema);
    s.append(" .query_preds=" + query_preds);
    s.append(" .index_preds=" + index_preds);
    s.append(" .index2_preds=" + index2_preds);
    return s;
  }
};
WRITE_CLASS_ENCODER(query_op)

struct test_op {

  // query parameters (old)
  std::string query;   // query name
  double extended_price;
  int order_key;
  int line_number;
  int ship_date_low;
  int ship_date_high;
  double discount_low;
  double discount_high;
  double quantity;
  std::string comment_regex;
  bool use_index;
  bool old_projection;
  uint64_t extra_row_cost;
  bool fastpath;

  test_op() {}

  // serialize the fields into bufferlist to be sent over the wire
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(query, bl);
    ::encode(extended_price, bl);
    ::encode(order_key, bl);
    ::encode(line_number, bl);
    ::encode(ship_date_low, bl);
    ::encode(ship_date_high, bl);
    ::encode(discount_low, bl);
    ::encode(discount_high, bl);
    ::encode(quantity, bl);
    ::encode(comment_regex, bl);
    ::encode(use_index, bl);
    ::encode(old_projection, bl);
    ::encode(extra_row_cost, bl);
    ::encode(fastpath, bl);
    ENCODE_FINISH(bl);
  }

  // deserialize the fields from the bufferlist into this struct
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(query, bl);
    ::decode(extended_price, bl);
    ::decode(order_key, bl);
    ::decode(line_number, bl);
    ::decode(ship_date_low, bl);
    ::decode(ship_date_high, bl);
    ::decode(discount_low, bl);
    ::decode(discount_high, bl);
    ::decode(quantity, bl);
    ::decode(comment_regex, bl);
    ::decode(use_index, bl);
    ::decode(old_projection, bl);
    ::decode(extra_row_cost, bl);
    ::decode(fastpath, bl);
    DECODE_FINISH(bl);
  }

  std::string toString() {
    std::string s;
    s.append("test_op:");
    s.append(" .query=" + query);
    s.append(" .extended_price=" + std::to_string(extended_price));
    s.append(" .order_key=" + std::to_string(order_key));
    s.append(" .line_number=" + std::to_string(line_number));
    s.append(" .ship_date_low=" + std::to_string(ship_date_low));
    s.append(" .ship_date_high=" + std::to_string(ship_date_high));
    s.append(" .discount_low=" + std::to_string(discount_low));
    s.append(" .discount_high=" + std::to_string(discount_high));
    s.append(" .quantity=" + std::to_string(quantity));
    s.append(" .comment_regex=" + comment_regex);
    s.append(" .use_index=" + std::to_string(use_index));
    s.append(" .old_projection=" + std::to_string(old_projection));
    s.append(" .extra_row_cost=" + std::to_string(extra_row_cost));
    s.append(" .fastpath=" + std::to_string(fastpath));
    return s;
  }
};
WRITE_CLASS_ENCODER(test_op)


struct stats_op {

  std::string db_schema;
  std::string table_name;
  std::string data_schema;

  stats_op() {}
  stats_op(std::string dbscma, std::string tname, std::string dtscma) :
           db_schema(dbscma), table_name(tname), data_schema(dtscma) { }

  // serialize the fields into bufferlist to be sent over the wire
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(db_schema, bl);
    ::encode(table_name, bl);
    ::encode(data_schema, bl);
    ENCODE_FINISH(bl);
  }

  // deserialize the fields from the bufferlist into this struct
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(db_schema, bl);
    ::decode(table_name, bl);
    ::decode(data_schema, bl);
    DECODE_FINISH(bl);
  }

  std::string toString() {
    std::string s;
    s.append("stats_op:");
    s.append(" .db_schema=" + db_schema);
    s.append(" .table_name=" + table_name);
    s.append(" .data_schema=" + data_schema);
    return s;
  }
};
WRITE_CLASS_ENCODER(stats_op)

struct transform_op {

  std::string table_name;
  std::string query_schema;
  int required_type;

  transform_op() {}
  transform_op(std::string tname, std::string qrscma, int req_type) :
    table_name(tname), query_schema(qrscma), required_type(req_type) { }

  // serialize the fields into bufferlist to be sent over the wire
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(table_name, bl);
    ::encode(query_schema, bl);
    ::encode(required_type, bl);
    ENCODE_FINISH(bl);
  }

  // deserialize the fields from the bufferlist into this struct
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(table_name, bl);
    ::decode(query_schema, bl);
    ::decode(required_type, bl);
    DECODE_FINISH(bl);
  }

  std::string toString() {
    std::string s;
    s.append("transform_op:");
    s.append(" .table_name=" + table_name);
    s.append(" .query_schema=" + query_schema);
    s.append(" .required_type=" + std::to_string(required_type));
    return s;
  }
};
WRITE_CLASS_ENCODER(transform_op)


struct hep_file_metadata {

  // parent information for this file
  std::string dataset_name;

  // store logical and physical metadata as KV pairs in json strings.
  // https://root.cern.ch/doc/master/classTFile.html
  // physical offsets into the file.
  std::string tfile_header;  // json - physical file info

  // https://root.cern.ch/doc/master/classTTree.html
  // logical info how many trees in file and branches in each tree
  std::string file_schema;  // json - logical file info

  hep_file_metadata() {}
  hep_file_metadata(
    std::string _dataset_name,
    std::string _tfile_header,
    std::string _file_schema) :
        dataset_name(_dataset_name),
        tfile_header(_tfile_header),
        file_schema(_file_schema) { }

  // serialize the fields into bufferlist to be sent over the wire
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(dataset_name, bl);
    ::encode(tfile_header, bl);
    ::encode(file_schema, bl);
    ENCODE_FINISH(bl);
  }

  // deserialize the fields from the bufferlist into this struct
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(dataset_name, bl);
    ::decode(tfile_header, bl);
    ::decode(file_schema, bl);
    DECODE_FINISH(bl);
  }

  std::string toString() {
    std::string s;
    s.append("hep_file_metadata:");
    s.append(" .dataset_name=" + dataset_name);
    s.append(" .tfile_header=" + tfile_header);
    s.append(" .file_schema=" + file_schema);
    return s;
  }
};
WRITE_CLASS_ENCODER(hep_file_metadata);


// query op instructions for High Energy Physics (HEP) domain.
struct hep_query_op {

  std::string file_type; // e.g., root, nano_aod, etc.
  std::string query_schema;
  bool is_compressed;
  bool is_orig_read;  // i.e., off,len of original file data
  int orig_off;
  int orig_len;

  hep_query_op() {}
  hep_query_op(
    std::string _query_schema,
    bool _is_compressed,
    bool _is_orig_read,
    int _orig_off,
    int _orig_len) :
        query_schema(_query_schema),
        is_compressed(_is_compressed),
        is_orig_read(_is_orig_read),
        orig_off(_orig_off),
        orig_len(_orig_len)  { }

  // serialize the fields into bufferlist to be sent over the wire
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(query_schema, bl);
    ::encode(is_compressed, bl);
    ::encode(is_orig_read, bl);
    ::encode(orig_off, bl);
    ::encode(orig_len, bl);
    ENCODE_FINISH(bl);
  }

  // deserialize the fields from the bufferlist into this struct
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(query_schema, bl);
    ::decode(is_compressed, bl);
    ::decode(is_orig_read, bl);
    ::decode(orig_off, bl);
    ::decode(orig_len, bl);
    DECODE_FINISH(bl);
  }

  std::string toString() {
    std::string s;
    s.append("hep_query_op:");
    s.append(" .query_schema=" + query_schema);
    s.append(" .is_compressed=" + std::to_string(is_compressed));
    s.append(" .is_orig_read=" + std::to_string(is_orig_read));
    s.append(" .orig_off=" + std::to_string(orig_off));
    s.append(" .orig_len=" + std::to_string(orig_len));
    return s;
  }
};
WRITE_CLASS_ENCODER(hep_query_op);

// holds an omap entry containing flatbuffer location
// this entry type contains physical location info
// idx_key = idx_prefix + fb sequence number (int)
// val = this struct containing to PHYSICAL location of fb within obj
// note: objs contain a sequence of fbs, hence the off/len is needed
struct idx_fb_entry {
    uint32_t off;
    uint32_t len;

    idx_fb_entry() {}
    idx_fb_entry(uint32_t o, uint32_t l) : off(o), len(l) { }

    void encode(bufferlist& bl) const {
        ENCODE_START(1, 1, bl);
        ::encode(off, bl);
        ::encode(len, bl);
        ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator& bl) {
        DECODE_START(1, bl);
        ::decode(off, bl);
        ::decode(len, bl);
        DECODE_FINISH(bl);
    }

    std::string toString() {
        std::string s;
        s.append("idx_fb_entry.off=" + std::to_string(off));
        s.append("; idx_fb_entry.len=" + std::to_string(len));
        return s;
    }
};
WRITE_CLASS_ENCODER(idx_fb_entry)

// holds an omap entry for indexed col values
// this index entry type contains logical location info
// idx_key = idx_prefix + column data value(s) (ints)
// val = this struct containing to LOGICAL location of row within an fb
struct idx_rec_entry {
    uint32_t fb_num;
    uint32_t row_num;
    uint64_t rid;

    idx_rec_entry() {}
    idx_rec_entry(uint32_t fb, uint32_t row, uint64_t rec_id) :
            fb_num(fb),
            row_num(row),
            rid(rec_id){}

    void encode(bufferlist& bl) const {
        ENCODE_START(1, 1, bl);
        ::encode(fb_num, bl);
        ::encode(row_num, bl);
        ::encode(rid, bl);
        ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator& bl) {
        DECODE_START(1, bl);
        ::decode(fb_num, bl);
        ::decode(row_num, bl);
        ::decode(rid, bl);
        DECODE_FINISH(bl);
    }

    std::string toString() {
        std::string s;
        s.append("idx_rec_entry.fb_num=" + std::to_string(fb_num));
        s.append("; idx_rec_entry.row_num=" + std::to_string(row_num));
        s.append("; idx_rec_entry.rid=" + std::to_string(rid));
        return s;
    }
};
WRITE_CLASS_ENCODER(idx_rec_entry)

// omap entry for text index
// this index entry type contains logical location info
// idx_key = idx_prefix + column data value (here a text string)
// val = this struct containing to logical location of row within fb
struct idx_txt_entry {
    uint32_t fb_num;
    uint32_t row_num;
    uint64_t rid;
    uint32_t wpos;  // word's relative position in column, i.e., word number

    idx_txt_entry() {}
    idx_txt_entry(uint32_t fb, uint32_t row, uint64_t rec_id, uint32_t wp):
            fb_num(fb),
            row_num(row),
            rid(rec_id),
            wpos(wp) {}

    void encode(bufferlist& bl) const {
        ENCODE_START(1, 1, bl);
        ::encode(fb_num, bl);
        ::encode(row_num, bl);
        ::encode(rid, bl);
        ::encode(wpos, bl);
        ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator& bl) {
        DECODE_START(1, bl);
        ::decode(fb_num, bl);
        ::decode(row_num, bl);
        ::decode(rid, bl);
        ::decode(wpos, bl);
        DECODE_FINISH(bl);
    }

    std::string toString() {
        std::string s;
        s.append("idx_txt_entry.fb_num=" + std::to_string(fb_num));
        s.append("; idx_txt_entry.row_num=" + std::to_string(row_num));
        s.append("; idx_txt_entry.rid=" + std::to_string(rid));
        s.append("; idx_txt_entry.wpos=" + std::to_string(wpos));
        return s;
    }
};
WRITE_CLASS_ENCODER(idx_txt_entry)

// Stores index instructions/metadata into bl for build_sky_index()
struct idx_op {
    bool idx_unique;   // if idx contains all primary key/unique cols
    bool idx_ignore_stopwords;  // for text indexing
    uint32_t idx_batch_size;  // num idx entries to write into omap at once
    int idx_type;
    std::string idx_schema_str;
    std::string idx_text_delims; // for text indexing

    idx_op() {}
    idx_op(bool unq, bool ign, int batsz, int index_type,
           std::string schema_str, std::string delimiters) :
        idx_unique(unq),
        idx_ignore_stopwords(ign),
        idx_batch_size(batsz),
        idx_type(index_type),
        idx_schema_str(schema_str),
        idx_text_delims(delimiters) {}

    void encode(bufferlist& bl) const {
        ENCODE_START(1, 1, bl);
        ::encode(idx_unique, bl);
        ::encode(idx_ignore_stopwords, bl);
        ::encode(idx_batch_size, bl);
        ::encode(idx_type, bl);
        ::encode(idx_schema_str, bl);
        ::encode(idx_text_delims, bl);
        ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator& bl) {
        std::string s;
        DECODE_START(1, bl);
        ::decode(idx_unique, bl);
        ::decode(idx_ignore_stopwords, bl);
        ::decode(idx_batch_size, bl);
        ::decode(idx_type, bl);
        ::decode(idx_schema_str, bl);
        ::decode(idx_text_delims, bl);
        DECODE_FINISH(bl);
    }

    std::string toString() {
        std::string s;
        s.append("idx_op.idx_unique=" + std::to_string(idx_unique));
        s.append("idx_op.idx_ignore_stopwords=" +
                 std::to_string(idx_ignore_stopwords));
        s.append("; idx_op.idx_batch_size=" + std::to_string(idx_batch_size));
        s.append("; idx_op.idx_type=" + std::to_string(idx_type));
        s.append("; idx_op.idx_schema_str=\n" + idx_schema_str);
        s.append("; idx_op.text_delims=\n" + idx_text_delims);
        return s;
    }
};
WRITE_CLASS_ENCODER(idx_op)


// Stores column level statstics
struct col_stats {
    int col_id;     // fixed, refers to col in original schema
    int col_type;
    int table_id;
    int stats_level;  // an enum type for sampling density of stats collected
    int64_t utc;   // last time stats computed, use 0 to set to cur localtime
    string table_name;  // TODO: remove when table ids available.
    std::string col_info_str;  // datatype, etc. from col info struct.
    std::string min_val;
    std::string max_val;
    unsigned int nbins;
    std::vector<int> hist;  // TODO: should support uint type also

    col_stats() {}
    col_stats(int cid, int type, int tid, int level, int64_t cur_time,
              std::string tname, std::string cinfo, std::string min,
              std::string max, unsigned num_bins, std::vector<int> h) :
        col_id(cid),
        col_type(type),
        table_id(tid),
        stats_level(level),
        utc(cur_time),
        table_name(tname),
        col_info_str(cinfo),
        min_val(min),
        max_val(max),
        nbins(num_bins) {
            assert (nbins <= h.size());
            for (unsigned int i=0; i<nbins; i++) {
                hist.push_back(h[i]);
            }
            if (utc == 0) {
                std::time_t t = std::time(nullptr);
                utc = static_cast<long long int>(t);
            }
        }

    void encode(bufferlist& bl) const {
        ENCODE_START(1, 1, bl);
        ::encode(col_id, bl);
        ::encode(col_type, bl);
        ::encode(table_id, bl);
        ::encode(stats_level, bl),
        ::encode(utc, bl);
        ::encode(table_name, bl);
        ::encode(col_info_str, bl);
        ::encode(min_val, bl);
        ::encode(max_val, bl);
        ::encode(nbins, bl);
        for (unsigned int i=0; i<nbins; i++) {
            ::encode(hist[i], bl);
        }
        ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator& bl) {
        std::string s;
        DECODE_START(1, bl);
        ::decode(col_id, bl);
        ::decode(col_type, bl);
        ::decode(table_id, bl);
        ::decode(stats_level, bl);
        ::decode(utc, bl);
        ::decode(table_name, bl);
        ::decode(col_info_str, bl);
        ::decode(min_val, bl);
        ::decode(max_val, bl);
        ::decode(nbins, bl);
        for (unsigned int i=0; i<nbins; i++) {
            int tmp;
            ::decode(tmp, bl);
            hist.push_back(tmp);
        }
        DECODE_FINISH(bl);
    }

    std::string toString() {
        std::string s;
        s.append("col_stats.col_id=" + std::to_string(col_id));
        s.append("col_stats.col_type=" + std::to_string(col_type));
        s.append("col_stats.table_id=" + std::to_string(table_id));
        s.append("col_stats.stats_level=" + std::to_string(stats_level));
        s.append("col_stats.utc=" + std::to_string(utc));
        s.append("col_stats.table_name=" + table_name);
        s.append("col_stats.col_info_str=" + col_info_str);
        s.append("col_stats.min_val=" + min_val);
        s.append("col_stats.max_val=" + max_val);
        s.append("col_stats.nbins=" + std::to_string(nbins));
        s.append("col_stats.hist<");
        for (unsigned int i=0; i<nbins; i++) {
            s.append(std::to_string(hist[i]) + ",");
        }
        s.append(">");
        return s;
    }
};
WRITE_CLASS_ENCODER(col_stats)

// Example struct to store and serialize read/write info
// for custom cls class methods
struct inbl_sample_op {

  std::string message;
  std::string instructions;
  int counter;
  int func_id;

  inbl_sample_op() {}
  inbl_sample_op(
    std::string _message,
    std::string _instructions,
    int _counter,
    int _func_id)
    :
    message(_message),
    instructions(_instructions),
    counter(_counter),
    func_id(_func_id) { }

  // serialize the fields into bufferlist to be sent over the wire
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(message, bl);
    ::encode(instructions, bl);
    ::encode(counter, bl);
    ::encode(func_id, bl);
    ENCODE_FINISH(bl);
  }

  // deserialize the fields from the bufferlist into this struct
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(message, bl);
    ::decode(instructions, bl);
    ::decode(counter, bl);
    ::decode(func_id, bl);
    DECODE_FINISH(bl);
  }

  std::string toString() {
    std::string s;
    s.append("inbl_sample_op:");
    s.append(" .message=" + message);
    s.append(" .instructions=" + instructions);
    s.append(" .counter=" + std::to_string(counter));
    s.append(" .func_id=" + std::to_string(func_id));
    return s;
  }
};
WRITE_CLASS_ENCODER(inbl_sample_op)

// Example struct to store and serialize output info that
// can be returned from custom cls class read/write methods.
struct outbl_sample_info {

  std::string message;
  int rows_processed;
  uint64_t read_time_ns;
  uint64_t eval_time_ns;

  outbl_sample_info() {}
  outbl_sample_info(
    std::string _message,
    int _rows_processed,
    uint64_t _read_time_ns,
    uint64_t _eval_time_ns)
    :
    message(_message),
    rows_processed(_rows_processed),
    read_time_ns(_read_time_ns),
    eval_time_ns(_eval_time_ns) { }

  // serialize the fields into bufferlist to be sent over the wire
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(message, bl);
    ::encode(rows_processed, bl);
    ::encode(read_time_ns, bl);
    ::encode(eval_time_ns, bl);
    ENCODE_FINISH(bl);
  }

  // deserialize the fields from the bufferlist into this struct
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(message, bl);
    ::decode(rows_processed, bl);
    ::decode(read_time_ns, bl);
    ::decode(eval_time_ns, bl);
    DECODE_FINISH(bl);
  }

  std::string toString() {
    std::string s;
    s.append("outbl_sample_info:");
    s.append(" .message=" + message);
    s.append(" .rows_processed=" + rows_processed);
    s.append(" .read_time_ns=" + std::to_string(read_time_ns));
    s.append(" .eval_time_ns=" + std::to_string(eval_time_ns));
    return s;
  }
};
WRITE_CLASS_ENCODER(outbl_sample_info)

struct wasm_inbl_sample_op {

  std::string message;
  std::string instructions;
  int counter;
  int func_id;

  wasm_inbl_sample_op() {}
  wasm_inbl_sample_op(
    std::string _message,
    std::string _instructions,
    int _counter,
    int _func_id)
    :
    message(_message),
    instructions(_instructions),
    counter(_counter),
    func_id(_func_id) { }

  // serialize the fields into bufferlist to be sent over the wire
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(message, bl);
    ::encode(instructions, bl);
    ::encode(counter, bl);
    ::encode(func_id, bl);
    ENCODE_FINISH(bl);
  }

  // deserialize the fields from the bufferlist into this struct
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(message, bl);
    ::decode(instructions, bl);
    ::decode(counter, bl);
    ::decode(func_id, bl);
    DECODE_FINISH(bl);
  }

  std::string toString() {
    std::string s;
    s.append("inbl_sample_op:");
    s.append(" .message=" + message);
    s.append(" .instructions=" + instructions);
    s.append(" .counter=" + std::to_string(counter));
    s.append(" .func_id=" + std::to_string(func_id));
    return s;
  }
};
WRITE_CLASS_ENCODER(wasm_inbl_sample_op)

// Example struct to store and serialize output info that
// can be returned from custom cls class read/write methods.
struct wasm_outbl_sample_info {

  std::string message;
  int rows_processed;
  uint64_t read_time_ns;
  uint64_t eval_time_ns;

  wasm_outbl_sample_info() {}
  wasm_outbl_sample_info(
    std::string _message,
    int _rows_processed,
    uint64_t _read_time_ns,
    uint64_t _eval_time_ns)
    :
    message(_message),
    rows_processed(_rows_processed),
    read_time_ns(_read_time_ns),
    eval_time_ns(_eval_time_ns) { }

  // serialize the fields into bufferlist to be sent over the wire
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(message, bl);
    ::encode(rows_processed, bl);
    ::encode(read_time_ns, bl);
    ::encode(eval_time_ns, bl);
    ENCODE_FINISH(bl);
  }

  // deserialize the fields from the bufferlist into this struct
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(message, bl);
    ::decode(rows_processed, bl);
    ::decode(read_time_ns, bl);
    ::decode(eval_time_ns, bl);
    DECODE_FINISH(bl);
  }

  std::string toString() {
    std::string s;
    s.append("outbl_sample_info:");
    s.append(" .message=" + message);
    s.append(" .rows_processed=" + rows_processed);
    s.append(" .read_time_ns=" + std::to_string(read_time_ns));
    s.append(" .eval_time_ns=" + std::to_string(eval_time_ns));
    return s;
  }
};
WRITE_CLASS_ENCODER(wasm_outbl_sample_info)

// Custom op struct for HEP data queries.
struct hep_op {

  bool fastpath;
  std::string dataset_name;
  std::string file_name;
  std::string tree_name;
  std::string data_schema;
  std::string query_schema;
  std::string query_preds;

  hep_op() {}
  hep_op(
    bool _fastpath,
    std::string _dataset_name,
    std::string _file_name,
    std::string _tree_name,
    std::string _data_schema,
    std::string _query_schema,
    std::string _query_preds)
    :
    fastpath(_fastpath),
    dataset_name(_dataset_name),
    file_name(_file_name),
    tree_name(_tree_name),
    data_schema(_data_schema),
    query_schema(_query_schema),
    query_preds(_query_preds) { }

  // serialize the fields into bufferlist to be sent over the wire
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(fastpath, bl);
    ::encode(dataset_name, bl);
    ::encode(file_name, bl);
    ::encode(tree_name, bl);
    ::encode(data_schema, bl);
    ::encode(query_schema, bl);
    ::encode(query_preds, bl);
    ENCODE_FINISH(bl);
  }

  // deserialize the fields from the bufferlist into this struct
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(fastpath, bl);
    ::decode(dataset_name, bl);
    ::decode(file_name, bl);
    ::decode(tree_name, bl);
    ::decode(data_schema, bl);
    ::decode(query_schema, bl);
    ::decode(query_preds, bl);
    DECODE_FINISH(bl);
  }

  std::string toString() {
    std::string s;
    s.append("hep_op:");
    s.append(" .fastpath=" + std::to_string(fastpath));
    s.append(" .dataset_name=" + dataset_name);
    s.append(" .file_name=" + file_name);
    s.append(" .tree_name=" + tree_name);
    s.append(" .data_schema=" + data_schema);
    s.append(" .query_schema=" + query_schema);
    s.append(" .query_preds=" + query_preds);
    return s;
  }
};
WRITE_CLASS_ENCODER(hep_op)


// Locking object information, used for cross-object atomic
// exclusive lock mechanism.
struct lockobj_info {
  bool table_busy;
  int num_objs;
  std::string table_name;
  std::string table_group;

  lockobj_info() {}
  lockobj_info(
    bool _table_busy,
    int _num_objs,
    std::string _table_name,
    std::string _table_group)
    :
    table_busy(_table_busy),
    num_objs(_num_objs),
    table_name(_table_name),
    table_group(_table_group) { }

  // serialize the fields into bufferlist to be sent over the wire
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(table_busy, bl);
    ::encode(num_objs, bl);
    ::encode(table_name, bl);
    ::encode(table_group, bl);
    ENCODE_FINISH(bl);
  }

  // deserialize the fields from the bufferlist into this struct
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(table_busy, bl);
    ::decode(num_objs, bl);
    ::decode(table_name, bl);
    ::decode(table_group, bl);
    DECODE_FINISH(bl);
  }

  std::string toString() {
    std::string s;
    s.append(" lockobj_info:");
    s.append(" .table_busy=" + table_busy);
    s.append(" .num_objs=" + num_objs);
    s.append(" .table_name=" + table_name);
    s.append(" .table_group=" + table_group);
    return s;
  }
};
WRITE_CLASS_ENCODER(lockobj_info)

// Used to collect runtime information in CLS during processing tasks.
struct cls_info {
  uint64_t rows_processed;
  uint64_t read_ns;
  uint64_t eval_ns;
  std::string push_back_predicates;
  std::string push_back_reason;

  cls_info() {}
  cls_info(
    uint64_t _read_ns,
    uint64_t _eval_ns,
    std::string _push_back_predicates,
    std::string _push_back_reason)
    :
    read_ns(_read_ns),
    eval_ns(_eval_ns),
    push_back_predicates(_push_back_predicates),
    push_back_reason(_push_back_reason) { }

  // serialize the fields into bufferlist to be sent over the wire
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(read_ns, bl);
    ::encode(eval_ns, bl);
    ::encode(push_back_predicates, bl);
    ::encode(push_back_reason, bl);
    ENCODE_FINISH(bl);
  }

  // deserialize the fields from the bufferlist into this struct
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(read_ns, bl);
    ::decode(eval_ns, bl);
    ::decode(push_back_predicates, bl);
    ::decode(push_back_reason, bl);
    DECODE_FINISH(bl);
  }

  std::string toString() {
    std::string s;
    s.append(" cls_info:");
    s.append(" .read_ns=" + std::to_string(read_ns));
    s.append(" .eval_ns=" + std::to_string(eval_ns));
    s.append(" .push_back_predicates=" + push_back_predicates);
    s.append(" .push_back_reason=" + push_back_reason);
    return s;
  }
};
WRITE_CLASS_ENCODER(cls_info)


#endif
