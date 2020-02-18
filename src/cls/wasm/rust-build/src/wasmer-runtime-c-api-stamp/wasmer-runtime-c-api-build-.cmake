

set(command "/usr/bin/cmake;-Dmake=${make};-Dconfig=${config};-P;/mnt/sda4/skyhookdm-ceph/src/cls/wasm/rust-build/src/wasmer-runtime-c-api-stamp/wasmer-runtime-c-api-build--impl.cmake")
execute_process(
  COMMAND ${command}
  RESULT_VARIABLE result
  OUTPUT_FILE "/mnt/sda4/skyhookdm-ceph/src/cls/wasm/rust-build/src/wasmer-runtime-c-api-stamp/wasmer-runtime-c-api-build-out.log"
  ERROR_FILE "/mnt/sda4/skyhookdm-ceph/src/cls/wasm/rust-build/src/wasmer-runtime-c-api-stamp/wasmer-runtime-c-api-build-err.log"
  )
if(result)
  set(msg "Command failed: ${result}\n")
  foreach(arg IN LISTS command)
    set(msg "${msg} '${arg}'")
  endforeach()
  set(msg "${msg}\nSee also\n  /mnt/sda4/skyhookdm-ceph/src/cls/wasm/rust-build/src/wasmer-runtime-c-api-stamp/wasmer-runtime-c-api-build-*.log")
  message(FATAL_ERROR "${msg}")
else()
  set(msg "wasmer-runtime-c-api build command succeeded.  See also /mnt/sda4/skyhookdm-ceph/src/cls/wasm/rust-build/src/wasmer-runtime-c-api-stamp/wasmer-runtime-c-api-build-*.log")
  message(STATUS "${msg}")
endif()
