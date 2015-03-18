
add_executable(unittests
  buffer_trader.cpp
  data_reader.cpp
  plugin.cpp
  test.cpp
  ../mapredo/directory.cpp)

target_link_libraries(unittests
  lmapredo
  ${GTEST_BOTH_LIBRARIES}
  ${CMAKE_DL_LIBS})
include_directories(../mapredo)

add_test (all_tests unittests)
