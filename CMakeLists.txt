cmake_minimum_required (VERSION 2.8.5)

project (mapredo)
set(PROJECT_VERSION 0.6.0)

if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release")
endif()

if (CMAKE_COMPILER_IS_GNUCXX)
  set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -g -pedantic -Wall -Wno-unused-result -std=c++11")
endif()

#message(${CMAKE_CXX_FLAGS_RELEASE} ${CMAKE_CXX_FLAGS})

include_directories (BEFORE ${PROJECT_SOURCE_DIR})

set (ARCHIVE_NAME ${CMAKE_PROJECT_NAME}-${PROJECT_VERSION})
add_custom_target (dist
  COMMAND git archive --prefix=${ARCHIVE_NAME}/ HEAD
  | bzip2 > ${CMAKE_BINARY_DIR}/${ARCHIVE_NAME}.tar.bz2
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

#set (CPACK_GENERATOR "TBZ2")
#set (CPACK_SOURCE_GENERATOR "TBZ2")
#set (CPACK_SOURCE_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}-${PROJECT_VERSION}")
#set (CPACK_SOURCE_IGNORE_FILES
#  "/build/;pg100;\\\\.git;\\\\.gitignore;~$;${CPACK_SOURCE_IGNORE_FILES}")
#include (CPack)

include (GNUInstallDirs)

configure_file (
  "${mapredo_SOURCE_DIR}/mapredo.spec.in"
  "${mapredo_BINARY_DIR}/mapredo.spec")

enable_testing()
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIR})
add_custom_target(check COMMAND "tests/unittests")

find_package(Doxygen)

add_definitions(-DPLUGINSDIR="${CMAKE_INSTALL_FULL_LIBDIR}/mapredo")

add_subdirectory (frontend)
add_subdirectory (mapredo)
add_subdirectory (plugins)
add_subdirectory (tests)
add_subdirectory (doc)
