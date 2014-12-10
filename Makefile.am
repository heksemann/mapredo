#ACLOCAL_AMFLAGS = -I m4 --install
EXTRA_DIST = autogen.sh mapredo.spec mapredo.spec.in
SUBDIRS = mapredo frontend plugins test

pkg: dist
	meos-pkg $(PACKAGE)-$(VERSION).tar.gz

install-data-local:
	$(MKDIR_P) $(DESTDIR)$(localstatedir)/tmp/mapredo
