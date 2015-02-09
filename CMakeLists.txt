cmake_minimum_required (VERSION 2.8.5)

project (mapredo)

if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release")
endif()

if (CMAKE_COMPILER_IS_GNUCXX)
  set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -g -pedantic -Wall -Wno-unused-result -std=c++11")
endif()

include_directories(BEFORE ${PROJECT_SOURCE_DIR})

set(PROJECT_VERSION 0.2.0)
set(ARCHIVE_NAME ${CMAKE_PROJECT_NAME}-${PROJECT_VERSION})
add_custom_target(dist
  COMMAND git archive --prefix=${ARCHIVE_NAME}/ HEAD
  | bzip2 > ${CMAKE_BINARY_DIR}/${ARCHIVE_NAME}.tar.bz2
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

set (CPACK_PACKAGE_VERSION_MAJOR 0)
set (CPACK_PACKAGE_VERSION_MINOR 1)
set (CPACK_PACKAGE_VERSION_PATCH 0)
set (PACKAGE_VERSION
  "${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}")
  
set (CPACK_GENERATOR "TBZ2")
set (CPACK_SOURCE_GENERATOR "TBZ2")
set (CPACK_SOURCE_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}-${PACKAGE_VERSION}")
set (CPACK_SOURCE_IGNORE_FILES
  "/build/;pg100;\\\\.git;\\\\.gitignore;~$;${CPACK_SOURCE_IGNORE_FILES}")
include (CPack)

include (GNUInstallDirs)

configure_file (
  "${mapredo_SOURCE_DIR}/mapredo.spec.in"
  "${mapredo_BINARY_DIR}/mapredo.spec")

enable_testing()
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIR})

find_package(Doxygen)

add_definitions(-DPLUGINSDIR="${CMAKE_INSTALL_FULL_LIBDIR}/mapredo")

add_subdirectory (frontend)
add_subdirectory (mapredo)
add_subdirectory (plugins)
add_subdirectory (tests)
add_subdirectory (doc)
