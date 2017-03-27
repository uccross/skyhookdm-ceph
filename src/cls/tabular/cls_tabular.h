#ifndef CLS_TABULAR_H
#define CLS_TABULAR_H
#include "include/types.h"

struct scan_op {
  uint64_t max_val;
  bool use_index;
  bool naive;
  bool preallocate;

  scan_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(max_val, bl);
    ::encode(use_index, bl);
    ::encode(naive, bl);
    ::encode(preallocate, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(max_val, bl);
    ::decode(use_index, bl);
    ::decode(naive, bl);
    ::decode(preallocate, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(scan_op)

struct regex_scan_op {
  size_t row_size;
  size_t field_offset;
  size_t field_length;
  std::string regex;

  regex_scan_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(row_size, bl);
    ::encode(field_offset, bl);
    ::encode(field_length, bl);
    ::encode(regex, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(row_size, bl);
    ::decode(field_offset, bl);
    ::decode(field_length, bl);
    ::decode(regex, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(regex_scan_op)

struct query_op {
  // query name
  std::string query;

  // query parameters
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
    // deserialize the field from the bufferlist into this struct
    ::decode(extra_row_cost, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(query_op)

#endif
