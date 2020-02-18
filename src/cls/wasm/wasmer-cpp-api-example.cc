#include "rust-build/src/wasmer-runtime-c-api/lib/runtime-c-api/wasmer.hh"
#include <iostream>

using namespace std;
int main(){
        cout<<"Hello"<<endl;
        FILE *file = fopen("/tmp/doNothing.wasm", "r");
    fseek(file, 0, SEEK_END);
    long len = ftell(file);
    uint8_t *bytes = (uint8_t*) malloc(len);
    fseek(file, 0, SEEK_SET);
    fread(bytes, 1, len, file);
    fclose(file);

    wasmer_import_t imports[] = {};
    wasmer_instance_t *instance = NULL;
        cout<<"Ending"<<endl;
	cout<<"New"<<endl;
    	wasmer_result_t instantiation_result = wasmer_instantiate(&instance, bytes, len, imports, 0);
    return 0;
}
