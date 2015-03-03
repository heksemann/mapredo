include (GenerateExportHeader)  

add_library (wordcount MODULE wordcount.cpp)
set_target_properties(wordcount PROPERTIES PREFIX "")

add_library (wordsort MODULE wordsort.cpp)
set_target_properties(wordsort PROPERTIES PREFIX "")

install (TARGETS
  wordcount
  wordsort
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/mapredo)
