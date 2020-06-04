#ifndef CLS_WASM_H
#define CLS_WASM_H

#include <include/types.h>

struct wasm_inbl_sample_op {

  std::string wasm_func;
  std::string wasm_engine;
  std::string func_params;

  wasm_inbl_sample_op() {}
  wasm_inbl_sample_op(
    std::string _wasm_func,
    std::string _wasm_engine,
    std::string _func_params)
    :
    wasm_func(_wasm_func),
    wasm_engine(_wasm_engine),
    func_params(_func_params) { }

  // serialize the fields into bufferlist to be sent over the wire
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(wasm_func, bl);
    ::encode(wasm_engine, bl);
    ::encode(func_params, bl);
    ENCODE_FINISH(bl);
  }

  // deserialize the fields from the bufferlist into this struct
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(wasm_func, bl);
    ::decode(wasm_engine, bl);
    ::decode(func_params, bl);
    DECODE_FINISH(bl);
  }

  std::string toString() {
    std::string s;
    s.append("inbl_sample_op:");
    s.append(" .wasm_func=" + wasm_func);
    s.append(" .wasm_engine=" + wasm_engine);
    s.append(" .func_params=" + func_params);
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

#endif