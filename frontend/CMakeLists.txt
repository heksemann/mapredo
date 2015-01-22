
if (CMAKE_INSTALL_PREFIX STREQUAL /usr)
  set(CMAKE_INSTALL_FULL_LOCALSTATEDIR /var)
endif()

add_definitions(-DPACKAGE_VERSION="${PACKAGE_VERSION}")
add_definitions(-DTMPDIR="${CMAKE_INSTALL_FULL_LOCALSTATEDIR}/tmp/mapredo")

add_executable (mapredo main.cpp)

target_link_libraries (mapredo lmapredo ${CMAKE_DL_LIBS})

install(TARGETS mapredo DESTINATION bin)
