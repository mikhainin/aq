#!/bin/sh

sudo apt-get install fakeroot-ng git-buildpackage debhelper build-essential
BOOST_ROOT=/home/mgalanin/boost_1_57_0/ DH_VERBOSE=1 dpkg-buildpackage

install debian/aq/usr/bin/aq /var/www/utils/aq/aq
install debian/aq-dbg/usr/lib/debug/usr/bin/aq /var/www/utils/aq/aq.debug

cp ~/aq[-_]*.deb /var/www/debs/amd64/

cd /var/www/debs/amd64

dpkg-scanpackages . | gzip -9c > Packages.gz
