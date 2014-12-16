AC_INIT([mapredo], [0.1], [mapredo@hextreme.com], [mapredo], [http://hextreme.com/mapredo])
AC_PREREQ([2.59])
AM_INIT_AUTOMAKE([1.10 no-define foreign nostdinc silent-rules])
#AC_CONFIG_HEADERS([config.h])
AC_PROG_CXX
AC_LANG([C++])

AC_CHECK_HEADER([snappy.h],,
                [AC_MSG_ERROR([Unable to find Snappy header files.])])
AC_CHECK_HEADER([gtest/gtest.h],,
                [AC_MSG_ERROR([Unable to find Google Test header files.])])
AC_CHECK_HEADER([tclap/CmdLine.h],,
                [AC_MSG_ERROR([Unable to find tclap header files.])])

LT_INIT([disable-static dlopen])
AC_CONFIG_MACRO_DIR([m4])

#PKG_CHECK_MODULES(MAPREDO, [sigc++-2.0 glibmm-2.4])

AC_SUBST(AM_CXXFLAGS,
	 "-g -pedantic -Wall -Wno-unused-result -std=c++11")
# for valgrind --leak-check=full, use -O0 -fno-inline)

AC_CONFIG_FILES([Makefile
		 mapredo/Makefile
		 frontend/Makefile
		 plugins/Makefile
		 test/Makefile
		 mapredo.spec])
AC_OUTPUT
