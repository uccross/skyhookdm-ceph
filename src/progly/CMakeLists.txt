add_executable(tabular-datagen tabular-datagen.cc)
target_link_libraries(tabular-datagen librados global ${CMAKE_DL_LIBS}
    ${Boost_PROGRAM_OPTIONS_LIBRARY})
