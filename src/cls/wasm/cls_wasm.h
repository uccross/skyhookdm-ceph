/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#ifndef CLS_WASM_H
#define CLS_WASM_H

#include "include/types.h"

void cls_log_message(std::string msg, bool is_err, int log_level);
int example_func2();
int example_func3(string wasm_engine);
std::string GetStdoutFromCommand(string cmd);

// used by Arrow format only
#define STREAM_CAPACITY 1024
#define ARROW_RID_INDEX(cols) (cols)
#define ARROW_DELVEC_INDEX(cols) (cols + 1)


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

struct reg_wasm_func_op {

  std::string func_name;
  std::string func_binary;
  int func_len;

  reg_wasm_func_op() {}
  reg_wasm_func_op(
    std::string _func_name,
    std::string _func_binary,
    int _func_len)
    :
    func_name(_func_name),
    func_binary(_func_binary),
    func_len(_func_len) { }

  // serialize the fields into bufferlist to be sent over the wire
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(func_name, bl);
    ::encode(func_binary, bl);
    ::encode(func_len, bl);
    ENCODE_FINISH(bl);
  }

  // deserialize the fields from the bufferlist into this struct
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(func_name, bl);
    ::decode(func_binary, bl);
    ::decode(func_len, bl);
    DECODE_FINISH(bl);
  }

  std::string toString() {
    std::string s;
    s.append("reg_wasm_func_op:");
    s.append(" .func_name=" + func_name);
    s.append(" .func_binary=" + func_binary);
    s.append(" .func_len=" + func_len);
    return s;
  }
};
WRITE_CLASS_ENCODER(reg_wasm_func_op)


#endif
