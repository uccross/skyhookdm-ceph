add_executable(ceph_test_skyhook_query
  test_skyhook_query.cc
  )
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
  )
install(TARGETS
  ceph_test_skyhook_query
  DESTINATION ${CMAKE_INSTALL_BINDIR})
