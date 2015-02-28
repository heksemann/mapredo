if (RUBY_FOUND)
  execute_process(COMMAND ${RUBY_EXECUTABLE}
    -r rbconfig -e "dir=RbConfig::CONFIG['vendorlibdir'];
                    dir=RbConfig::CONFIG['rubylibdir'] if !dir;
                    print dir"
    OUTPUT_VARIABLE RUBY_VENDOR_LIBDIR)
  install (FILES mapredo.rb DESTINATION ${RUBY_VENDOR_LIBDIR})
endif()
