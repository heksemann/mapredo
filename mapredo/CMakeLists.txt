
add_library (lmapredo SHARED
  base.cpp
  directory.cpp
  engine.cpp
  file_merger.cpp
  settings.cpp
  sorter_buffer.cpp
  sorter.cpp)

set(lmapredo_VERSION_STRING 0.0.1)

set_target_properties(lmapredo
  PROPERTIES VERSION ${lmapredo_VERSION_STRING}
  SOVERSION 0
  OUTPUT_NAME mapredo)

target_link_libraries (lmapredo pthread snappy)

install (TARGETS lmapredo DESTINATION ${CMAKE_INSTALL_LIBDIR})
