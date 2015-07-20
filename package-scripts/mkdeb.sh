#!/bin/sh

sudo apt-get install fakeroot-ng git-buildpackage debhelper build-essential
git-buildpackage --git-upstream-tree=master --git-ignore-new --git-verbose

