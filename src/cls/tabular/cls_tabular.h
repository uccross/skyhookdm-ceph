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
#include "include/types.h"

/*
 * Stores the query request parameters.  This is encoded by the client and
 * decoded by server (osd node) for query processing.
 */
struct query_op {
  // query name
  std::string query;

  // query parameters (uses tpch lineitem schema)
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
  bool projection;
  bool fastpath;
  std::string table_schema_str;
  std::string query_schema_str;
  std::string predicate_str;
  uint64_t extra_row_cost;

  query_op() {}

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
    ::encode(projection, bl);
    ::encode(fastpath, bl);
    ::encode(table_schema_str, bl);
    ::encode(query_schema_str, bl);
    ::encode(predicate_str, bl);
    // serialize the field into bufferlist to be sent over the wire
    ::encode(extra_row_cost, bl);
    ENCODE_FINISH(bl);
  }

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
    ::decode(projection, bl);
    ::decode(fastpath, bl);
    ::decode(table_schema_str, bl);
    ::decode(query_schema_str, bl);
    ::decode(predicate_str, bl);
    // deserialize the field from the bufferlist into this struct
    ::decode(extra_row_cost, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(query_op)


// omap entry for indexed fb metadata
// key = fb sequence number
// val = struct containing physical location of fb within obj
// note that each fb is encoded as an independent bufferlist in the obj
// and objs contain a sequence of fbs
struct idx_fb_entry {
    uint32_t off;  // byte offset within obj pointing to encoded fb
    uint32_t len;  // fb size

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
};
WRITE_CLASS_ENCODER(idx_fb_entry)

// omap entry for indexed col value
// key = column data value (may be composite of multiple cols)
// val = struct containing to logical location of row within fb within obj
struct idx_val_entry {
    uint32_t fb_num;  // within obj containing seq of fbs
    uint32_t row_num;  // idx into rows array within fb root[nrows]

    idx_val_entry(uint32_t fb, uint32_t row) : fb_num(fb), row_num(row) {}

    void encode(bufferlist& bl) const {
        ENCODE_START(1, 1, bl);
        ::encode(fb_num, bl);
        ::encode(row_num, bl);
        ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator& bl) {
        DECODE_START(1, bl);
        ::decode(fb_num, bl);
        ::decode(row_num, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(idx_val_entry)


#endif
