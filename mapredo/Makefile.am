lib_LTLIBRARIES = libmapredo.la

AM_CXXFLAGS += -DPLUGINSDIR=\"$(libdir)/mapredo\"

libmapredo_la_SOURCES = \
	base.cpp \
	directory.cpp \
	engine.cpp \
	file_merger.cpp \
	sorter.cpp \
	sorter_buffer.cpp \
	settings.cpp

libmapredo_ladir = $(includedir)/mapredo
libmapredo_la_HEADERS = *.h

# current:revision:age
# API changes (new funcs)     : current++ age++ revision=0
# Bug fixes                   : revision++
# API breaks (old API changed): current++ age=0 revision=0
libmapredo_la_LDFLAGS = -version-info 1:0:0 -pthread
libmapredo_la_LIBADD = -ldl -lsnappy
