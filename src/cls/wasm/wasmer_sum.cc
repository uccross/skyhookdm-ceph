#include "rust-build/src/wasmer-runtime-c-api/lib/runtime-c-api/wasmer.hh"
//#include "rust-build/src/wasmer-runtime-c-api/lib/runtime-c-api/wasmer.h"
#include <iostream>

using namespace std;
int main(){
        cout<<"Hello"<<endl;
        FILE *file = fopen("/usr/local/bin/sum.wasm", "r");
    fseek(file, 0, SEEK_END);
    long len = ftell(file);
    uint8_t *bytes = (uint8_t*) malloc(len);
    fseek(file, 0, SEEK_SET);
    fread(bytes, 1, len, file);
    fclose(file);

    wasmer_import_t imports[] = {};
    wasmer_instance_t *instance = NULL;
    //cout<<"Ending"<<endl;
	//cout<<"New"<<endl;
    wasmer_result_t instantiation_result = wasmer_instantiate(&instance, bytes, len, imports, 0);


    wasmer_value_t argument_one;
    argument_one.tag = wasmer_value_tag::WASM_I32;
    argument_one.value.I32 = 7;

    wasmer_value_t argument_two;
    argument_two.tag = wasmer_value_tag::WASM_I32;
    argument_two.value.I32 = 8;
    // Prepare the arguments.
    wasmer_value_t arguments[] = {argument_one, argument_two};

    wasmer_value_t result_one;
    wasmer_value_t results[] = {result_one};
    // Call the `sum` function with the prepared arguments and the return value.
    wasmer_result_t call_result = wasmer_instance_call(instance, "sum", arguments, 2, results, 1);
    
    wasmer_instance_destroy(instance);
    return 0;
}