add_executable(skyhook-repart repart.cc)
target_link_libraries(skyhook-repart librados global ${Boost_PROGRAM_OPTIONS_LIBRARY})
install(TARGETS skyhook-repart DESTINATION bin)
