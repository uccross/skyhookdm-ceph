#include <errno.h>
#include <string>
#include <sstream>
#include <boost/lexical_cast.hpp>
#include <time.h>
#include "re2/re2.h"
#include "include/types.h"
#include "objclass/objclass.h"
#include "cls_wasm.h"
#include "wasm_export.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <fstream>

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

extern "C" {
  //char *bh_read_file_to_buffer(const char *filename, int *ret_size);
  void _bh_log() {}
  int foo(wasm_exec_env_t exec_env , int a, int b)
{
    return a+b;
}

void foo2(wasm_exec_env_t exec_env, 
          uint32_t msg_offset, 
          uint32_t buffer_offset, 
          int32_t buf_len)
{
    wasm_module_inst_t module_inst = get_module_inst(exec_env);
    char *buffer;
    char * msg ;

    // do boundary check
    if (!wasm_runtime_validate_app_str_addr(module_inst, msg_offset))
        return;
    
    if (!wasm_runtime_validate_app_addr(module_inst,buffer_offset, buf_len))
        return;
    //std::cout<<"Here!"<<std::endl;
    // do address conversion
    buffer = (char *)wasm_runtime_addr_app_to_native(module_inst, buffer_offset);
    msg = (char *) wasm_runtime_addr_app_to_native(module_inst, msg_offset);

    strncpy(buffer, msg, buf_len);
    //std::cout<<"buffer = "<<buffer<<std::endl;
}

}

char*
bh_read_file_to_buffer(const char *filename, int *ret_size)
{
    // char *buffer;
    // int file;
    // uint32_t file_size, read_size;
    // struct stat stat_buf;
    
    // if (!filename || !ret_size) {
    //     CLS_ERR("Read file to buffer failed: invalid filename or ret size.");
    //     return NULL;
    // }
    
    // if ((file = open(filename, O_RDONLY, 0)) == -1) {
    //     CLS_ERR("Read file to buffer failed: open file %s failed.\n",filename);
    //     return NULL;
    // }

    // if (stat(filename, &stat_buf) != 0) {
    //     CLS_ERR("Read file to buffer failed: fstat file %s failed.\n",filename);
    //     close(file);
    //     return NULL;
    // }
  
  

}
static
int wasm_query_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    // unpack the requested op from the inbl.
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

    wasm_func = "/usr/local/bin/test.wasm";
    CLS_LOG(20, "wasm_query_op: op.wasm_func = %s", wasm_func.c_str());
    CLS_LOG(20, "wasm_query_op: op.wasm_engine = %s", wasm_engine.c_str());
    CLS_LOG(20, "wasm_query_op: op.func_params = %s", func_params.c_str());

    


    int64_t read_timer = getns();
    int64_t func_timer = 0;
    int rows_processed;

    char *wasm_file = NULL;
    uint8_t *wasm_file_buf = NULL;
    int wasm_file_size;
    wasm_module_t wasm_module = NULL;

    
    wasm_module_inst_t wasm_module_inst = NULL;
    wasm_function_inst_t func;
    char error_buf[128];
    const char *exception;
    uint32_t func_argv[8] = { 0 };
    wasm_exec_env_t exec_env;
    uint32_t argv2[2];
    uint32_t argv3[4]={0};

    char * buffer = NULL;
    int32_t buffer_for_wasm;

    wasm_file = "/usr/local/bin/test.wasm";
    CLS_LOG(20, "wasm_query_op: wasm_file=%s", wasm_file);

    static NativeSymbol native_symbols[] = 
{
    EXPORT_WASM_API(foo),
    EXPORT_WASM_API(foo2),
};

    /* initialize runtime environment */
    if (!wasm_runtime_init())
        return 0;

    int n_native_symbols = sizeof(native_symbols) / sizeof(NativeSymbol);
    if (!wasm_runtime_register_natives("env",
                                   native_symbols, 
                                   n_native_symbols)) {
    return 0;
    }

    /* load WASM byte buffer from WASM bin file */
    //  if (!(wasm_file_buf = (uint8_t*) bh_read_file_to_buffer(wasm_file,
    //                                                          &wasm_file_size)))
    //      return 0;

    long size;

  ifstream file ("/usr/local/bin/test.wasm", ios::in|ios::binary|ios::ate);
  size = file.tellg();
  file.seekg (0, ios::beg);
  buffer = new char [size];
  file.read (buffer, size);
  file.close();
  wasm_file_size = (int)size;


  if (!(wasm_file_buf = (uint8_t*) buffer))
    return 0;
    

    /* load WASM module */
    if (!(wasm_module = wasm_runtime_load(wasm_file_buf, wasm_file_size,
                                          error_buf, sizeof(error_buf)))) {
        CLS_LOG(20,"Runtime load error ", error_buf);
        return 0;
    }

    /* instantiate the module */
    if (!(wasm_module_inst = wasm_runtime_instantiate(wasm_module,
                                                      32 * 1024, /* stack size */
                                                      32 * 1024,  /* heap size */
                                                      error_buf,
                                                      sizeof(error_buf)))) {
        CLS_LOG(20,"Instantiate error",error_buf );
        return 0;
    }

    func = wasm_runtime_lookup_function(wasm_module_inst, "_main", "(i32i32)i32");
    if (!func) {
        CLS_LOG(20,"Lookup function _main failed.");
        return 0;
    }

    exec_env = wasm_runtime_create_exec_env(wasm_module_inst, 32 * 1024);

    wasm_runtime_call_wasm(exec_env, func, 2, func_argv);
    if ((exception = wasm_runtime_get_exception(wasm_module_inst))) {
        CLS_LOG(20,"Got exception: ",exception);
        return 0;
    }

    func = wasm_runtime_lookup_function(wasm_module_inst, "_fib", "(i32)i32");
    if (!func) {
        CLS_LOG(20,"Lookup function _main failed." );
        return 0;
    }
    exec_env = wasm_runtime_create_exec_env(wasm_module_inst, 32 * 1024);

    func_argv[0] = 20;
    wasm_runtime_call_wasm(exec_env, func, 1, func_argv);
    if ((exception = wasm_runtime_get_exception(wasm_module_inst))) {
        CLS_LOG(20,"Got exception: ",exception);
        return 0;
    }
    CLS_LOG(20,"Call fib(20), return = %d" , func_argv[0] );

    func = wasm_runtime_lookup_function(wasm_module_inst, "_passString", "(i32)i32");
    if (!func) {
        CLS_LOG(20,"Lookup function _main failed.");
        return 0;
    }
    exec_env = wasm_runtime_create_exec_env(wasm_module_inst, 32 * 1024);

    buffer_for_wasm = wasm_runtime_module_malloc(wasm_module_inst, 10, (void**)&buffer);
    if(buffer_for_wasm != 0)
    {
        strncpy(buffer, "hello", 10);	// use native address for accessing in runtime
        //std::cout<<"buffer : "<<buffer<<std::endl;
        argv2[0] = buffer_for_wasm;  	// pass the buffer address for WASM space.
        argv2[1] = 10;					// the size of buffer
        wasm_runtime_call_wasm(exec_env, func, 2, argv2);
        if ((exception = wasm_runtime_get_exception(wasm_module_inst))) {
            CLS_LOG(20,"Got exception: ",exception);
            return 0;
        }
    // it is runtime responsibility to release the memory,
    // unless the WASM app will free the passed pointer in its code
    wasm_runtime_module_free(wasm_module_inst, buffer_for_wasm);
    }

    //////////////////////////////////////////////////////////////////////////////////////////

    func = wasm_runtime_lookup_function(wasm_module_inst, "_sum", "(i32i32)i32");
    if (!func) {
        CLS_LOG(20,"Lookup function _sum failed.");
        return 0;
    }
    exec_env = wasm_runtime_create_exec_env(wasm_module_inst, 32 * 1024);

    func_argv[0] = 2;
    func_argv[1] = 20;

    wasm_runtime_call_wasm(exec_env, func, 2, func_argv);
    if ((exception = wasm_runtime_get_exception(wasm_module_inst))) {
        CLS_LOG(20,"Got exception:%s ", exception );
        return 0;
    }
     CLS_LOG(20,"Call _sum(2,20), return = %d",func_argv[0]);



    wasm_runtime_deinstantiate(wasm_module_inst);

    uint64_t start = getns();
    rows_processed = 0;
    uint64_t end = getns();
    func_timer = end - start;
    CLS_LOG(20, "wasm_query_op: function result=%d", rows_processed);
    

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

void __cls_init()
{
  CLS_LOG(20, "Loaded wasm class!");

  cls_register("wasm", &h_class);

  cls_register_cxx_method(h_class, "wasm_query_op",
      CLS_METHOD_RD, wasm_query_op, &h_wasm_query_op);
}
