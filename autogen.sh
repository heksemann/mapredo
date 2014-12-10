rm -f config.cache config.status
aclocal -I m4 --install
libtoolize --force
#autoheader
automake -a
autoconf
echo 'run "./configure"'

