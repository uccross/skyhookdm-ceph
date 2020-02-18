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
#include <time.h>
#include "include/types.h"
#include "objclass/objclass.h"
#include "cls_wasm.h"
#include "rust-build/src/wasmer-runtime-c-api/lib/runtime-c-api/wasmer.hh"

CLS_VER(1,0)
CLS_NAME(wasm)

cls_handle_t h_class;
cls_method_handle_t h_wasm_query_op;

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

static
int wasm_query_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    wasm_inbl_sample_op op;
    try {
        bufferlist::iterator it = in->begin();
        ::decode(op, it);
    } catch (const buffer::error &err) {
        CLS_ERR("ERROR: cls_tabular:example_query_op: decoding inbl_sample_op");
        return -EINVAL;
    }

    std::string wasm_func = op.wasm_func;
    std::string wasm_engine = op.wasm_engine;
    std::string func_params = op.func_params;

    CLS_LOG(20, "wasm_query_op: op.wasm_func = %s", wasm_func.c_str());
    CLS_LOG(20, "wasm_query_op: op.wasm_engine = %s", wasm_engine.c_str());
    CLS_LOG(20, "wasm_query_op: op.func_params = %s", func_params.c_str());

    //using namespace Tables;
    //schema_vec data_schema = schemaFromString(op.data_schema);

    int64_t read_timer = getns();
    int64_t func_timer = 0;
    int rows_processed = 0;

    uint64_t start = getns();
    std::string command;
    command = GetStdoutFromCommand("/usr/local/bin/wasmer-c-api-example /usr/local/bin/doNothing.wasm; echo $?");
    //command = GetStdoutFromCommand("/usr/local/bin/wasmer-c-api-example /usr/local/bin/doNothing.wasm; echo $?");
    CLS_LOG(20, "command = %s",command.c_str());
    rows_processed = std::stoi(command);
    uint64_t end = getns();
    CLS_LOG(20, "wasm_query_op: function result=%d", rows_processed);

    int result2 = 0;

    if (func_params == "func2") {
        //result2 = example_func2();
        CLS_LOG(20, "Executing function 2");

        FILE *file = fopen("/usr/local/bin/doNothing.wasm", "r");
        
        fseek(file, 0, SEEK_END);
        long len = ftell(file);
        uint8_t *bytes = (uint8_t*) malloc(len);
        fseek(file, 0, SEEK_SET);
        fread(bytes, 1, len, file);
        fclose(file);

        wasmer_import_t imports[] = {};
        wasmer_instance_t *instance = NULL;
        wasmer_result_t instantiation_result = wasmer_instantiate(&instance, bytes, len, imports, 0);
        CLS_LOG(20, "Ending function 2, result = %d", instantiation_result);

        wasmer_value_t params[] = {};
        wasmer_value_t result_one;
        wasmer_value_t results[] = {result_one};
        wasmer_result_t call_result = wasmer_instance_call(instance, "doNothing", params, 0, results, 1);
        CLS_LOG(20, "Call result:  %d", call_result);
        
        wasmer_instance_destroy(instance);
        
    }
    
    if (func_params == "func3") {
      CLS_LOG(20, "Executing function 3");
      
      FILE *file = fopen("/usr/local/bin/sum.wasm", "r");
        
      fseek(file, 0, SEEK_END);
      long len = ftell(file);
      uint8_t *bytes = (uint8_t*) malloc(len);
      fseek(file, 0, SEEK_SET);
      fread(bytes, 1, len, file);
      fclose(file);
    
      wasmer_import_t imports[] = {};
      wasmer_instance_t *instance = NULL;
      wasmer_result_t instantiation_result = wasmer_instantiate(&instance, bytes, len, imports, 0);

      CLS_LOG(20, "Ending function 3, result = %d", instantiation_result);
      
      wasmer_value_t argument_one;
      argument_one.tag = wasmer_value_tag::WASM_I32;  
      argument_one.value.I32 = 7;

      wasmer_value_t argument_two;
      argument_two.tag = wasmer_value_tag::WASM_I32;
      argument_two.value.I32 = 8;

      wasmer_value_t arguments[] = {argument_one, argument_two};

      // Prepare the return value.
      wasmer_value_t result_one;
      wasmer_value_t results[] = {result_one};
      wasmer_result_t call_result = wasmer_instance_call(instance, "sum", arguments, 2, results, 1);
      CLS_LOG(20, "Call result in function 3:  %d", call_result);
      wasmer_instance_destroy(instance);

    }

    if (func_params == "func4"){
      CLS_LOG(20, "Executing function 4");
    }

    // encode output info for client.
    wasm_outbl_sample_info info;
    info.message = "All done with example op.";
    info.rows_processed = rows_processed;
    info.read_time_ns = read_timer;
    info.eval_time_ns = func_timer;
    ::encode(info, *out);

    // encode result data for client.
    bufferlist result_bl;
    result_bl.append("result data goes into result bl.");
    ::encode(result_bl, *out);

    return 0;
                     
}

int example_func2(){
        CLS_LOG(20, "Executing the binary file on the osd... in example_func2");
        int result = 1;

        return result;
}

std::string GetStdoutFromCommand(string cmd) {

        std::string data;
        FILE * stream;
        const int max_buffer = 256;
        char buffer[max_buffer];
        cmd.append(" 2>&1");

        stream = popen(cmd.c_str(), "r");
        if (stream) {
                while (!feof(stream))
                        if (fgets(buffer, max_buffer, stream) != NULL) data.append(buffer);
                        pclose(stream);
                }
        return data;
}


void __cls_init()
{
  CLS_LOG(20, "Loaded wasm class!");

  cls_register("wasm", &h_class);

  cls_register_cxx_method(h_class, "wasm_query_op",
      CLS_METHOD_RD, wasm_query_op, &h_wasm_query_op);
}

