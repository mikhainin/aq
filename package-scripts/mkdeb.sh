#!/bin/sh

sudo apt-get install fakeroot-ng git-buildpackage debhelper build-essential
BOOST_ROOT=/home/mgalanin/boost_1_57_0/ DH_VERBOSE=1 dpkg-buildpackage

