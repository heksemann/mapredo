AM_CXXFLAGS += -I$(top_srcdir)/mapredo -fprofile-arcs -ftest-coverage \
	-DPLUGINSDIR=\"../plugins/.libs\"

AM_LDFLAGS = -lgtest -fprofile-arcs -lsnappy -ldl -lpthread

TESTS = gtest
check_PROGRAMS = gtest

gtest_SOURCES = \
	test.cpp \
	plugin.cpp \
	../mapredo/directory.cpp

