
 if(!MSVC)  
  add_library (lmapredo SHARED
  base.cpp
  buffer_trader.cpp
  consumer.cpp
  directory.cpp
  engine.cpp
  file_merger.cpp
  settings.cpp
  sorter_buffer.cpp
  sorter.cpp)
else()
  add_library (lmapredo STATIC
  base.cpp
  buffer_trader.cpp
  consumer.cpp
  directory_win32.cpp
  engine.cpp
  file_merger.cpp
  settings.cpp
  sorter_buffer.cpp
  sorter.cpp)
endif()  
set(lmapredo_VERSION_STRING 0.0.1)

set_target_properties(lmapredo
  PROPERTIES VERSION ${lmapredo_VERSION_STRING}
  SOVERSION 0
  OUTPUT_NAME mapredo)

if(!MSVC)
  target_link_libraries (lmapredo pthread snappy)
else()
  target_link_libraries (lmapredo c:/users/kjirgens/HackdayII/mapredo/snappy-windows-1.1.1.8/native/snappy64.lib)
endif()

install (TARGETS lmapredo DESTINATION ${CMAKE_INSTALL_LIBDIR})
file (GLOB include_files "${CMAKE_CURRENT_SOURCE_DIR}/*.h")
install (FILES ${include_files} DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/mapredo)
