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

#endif
