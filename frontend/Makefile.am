bin_PROGRAMS = mapredo
mapredo_LDADD =  -lmapredo
mapredo_LDFLAGS = -L$(top_builddir)/mapredo

mapredo_SOURCES = main.cpp 

$(bin_PROGRAMS): $(lib_LTLIBRARIES)

AM_CXXFLAGS += \
	-I$(top_srcdir) \
	-DPLUGINSDIR=\"$(libdir)/mapredo\" \
	-DTMPDIR=\"$(localstatedir)/tmp/mapredo\"
