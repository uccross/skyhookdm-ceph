add_executable(tabular-datagen tabular-datagen.cc)
target_link_libraries(tabular-datagen librados global ${CMAKE_DL_LIBS}
    ${Boost_PROGRAM_OPTIONS_LIBRARY})
install(TARGETS tabular-datagen DESTINATION bin)

add_executable(tabular-scan tabular-scan.cc)
target_link_libraries(tabular-scan librados global ${CMAKE_DL_LIBS}
    ${Boost_PROGRAM_OPTIONS_LIBRARY})
install(TARGETS tabular-scan DESTINATION bin)

install(PROGRAMS filtering.sh DESTINATION bin
    RENAME tabular-filtering.sh)
