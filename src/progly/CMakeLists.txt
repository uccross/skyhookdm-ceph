# --------------------------------- #
add_executable(test_rowstransposecol test_rowstransposecol.cc 
                                     query_utils.cc 
                                     ${CMAKE_SOURCE_DIR}/src/cls/tabular/cls_tabular_utils.cc
                                     ${CMAKE_SOURCE_DIR}/src/cls/tabular/cls_transform_utils.cc )
target_link_libraries(test_rowstransposecol librados global ${CMAKE_DL_LIBS}
    ${Boost_PROGRAM_OPTIONS_LIBRARY} re2)
install(TARGETS test_rowstransposecol DESTINATION bin)

# --------------------------------- #
add_executable(test_fbvfile test_fbvfile.cc 
                            ${CMAKE_SOURCE_DIR}/src/cls/tabular/cls_test_utils.cc )
target_link_libraries(test_fbvfile librados global ${CMAKE_DL_LIBS}
    ${Boost_PROGRAM_OPTIONS_LIBRARY} re2)
install(TARGETS test_fbvfile DESTINATION bin)

# --------------------------------- #
add_executable(run-transforms run-transforms.cc query_utils.cc 
                                                ${CMAKE_SOURCE_DIR}/src/cls/tabular/cls_tabular_utils.cc
                                                ${CMAKE_SOURCE_DIR}/src/cls/tabular/cls_transform_utils.cc )
target_link_libraries(run-transforms librados global ${CMAKE_DL_LIBS}
    ${Boost_PROGRAM_OPTIONS_LIBRARY} re2)
install(TARGETS run-transforms DESTINATION bin)

# --------------------------------- #
add_executable(run-papertests run-papertests.cc query_utils.cc 
                                                ${CMAKE_SOURCE_DIR}/src/cls/tabular/cls_tabular_utils.cc
                                                ${CMAKE_SOURCE_DIR}/src/cls/tabular/cls_transform_utils.cc )
target_link_libraries(run-papertests librados global ${CMAKE_DL_LIBS}
    ${Boost_PROGRAM_OPTIONS_LIBRARY} re2)
install(TARGETS run-papertests DESTINATION bin)

# --------------------------------- #
add_executable(run-query run-query.cc query.cc ${CMAKE_SOURCE_DIR}/src/cls/tabular/cls_tabular_utils.cc)
target_link_libraries(run-query librados global ${CMAKE_DL_LIBS}
    ${Boost_PROGRAM_OPTIONS_LIBRARY} re2)
install(TARGETS run-query DESTINATION bin)

install(PROGRAMS filtering.sh DESTINATION bin
    RENAME tabular-filtering.sh)

install(PROGRAMS convert-tpch-tables.py DESTINATION bin)
install(PROGRAMS rados-store-glob.sh DESTINATION bin)

set(UNITTEST_LIBS gmock_main gmock gtest ${CMAKE_THREAD_LIBS_INIT} ${CMAKE_DL_LIBS})
set(UNITTEST_CXX_FLAGS "-I${CMAKE_SOURCE_DIR}/src/googletest/googlemock/include -I${CMAKE_BINARY_DIR}/src/googletest/googlemock/include -I${CMAKE_SOURCE_DIR}/src/googletest/googletest/include -I${CMAKE_BINARY_DIR}/src/googletest/googletest/include -fno-strict-aliasing")
add_executable(ceph_test_skyhook_query test_query.cc query.cc ${CMAKE_SOURCE_DIR}/src/cls/tabular/cls_tabular_utils.cc)
set_target_properties(ceph_test_skyhook_query PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_skyhook_query
  librados
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  radostest
  ${UNITTEST_LIBS}
  re2
  )
include_directories(${CMAKE_SOURCE_DIR}/src/googletest/googlemock/include)
install(TARGETS ceph_test_skyhook_query DESTINATION bin)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-function")
