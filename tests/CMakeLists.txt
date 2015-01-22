
add_executable(unittests
  test.cpp
  plugin.cpp
  ../mapredo/directory.cpp)

target_link_libraries(unittests
  lmapredo
  ${GTEST_BOTH_LIBRARIES}
  ${CMAKE_DL_LIBS})
include_directories(../mapredo)

add_test (all_tests unittests)
