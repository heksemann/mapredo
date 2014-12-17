AM_CPPFLAGS = -I$(top_srcdir)/mapredo -fprofile-arcs -ftest-coverage

AM_LDFLAGS = -lgtest -fprofile-arcs -lsnappy -ldl -lpthread

TESTS = gtest
check_PROGRAMS = gtest

gtest_SOURCES = \
	test.cpp \
	../mapredo/directory.cpp

