
add_definitions(-DPACKAGE_VERSION="${PACKAGE_VERSION}")
add_definitions(-DTMPDIR="${CMAKE_INSTALL_SHAREDSTATEDIR}/mapredo")

add_executable (mapredo main.cpp)

target_link_libraries (mapredo lmapredo ${CMAKE_DL_LIBS})

install(TARGETS mapredo DESTINATION bin)
