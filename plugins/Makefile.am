plugins_LTLIBRARIES = wordcount.la wordsort.la
AM_CPPFLAGS = -I$(top_srcdir)

wordcount_la_SOURCES = wordcount.cpp
wordcount_la_LDFLAGS = -module -avoid-version -shared -lmapredo \
	-L$(top_builddir)/mapredo
wordsort_la_SOURCES = wordsort.cpp
wordsort_la_LDFLAGS = -module -avoid-version -shared -lmapredo \
	-L$(top_builddir)/mapredo
pluginsdir = $(libdir)/mapredo

noinst_HEADERS = *.h
