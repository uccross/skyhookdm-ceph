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

#endif
