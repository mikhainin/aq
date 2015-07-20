#!/bin/sh
set +e
set -v
make -j6

objcopy --only-keep-debug aq aq.debug
strip -g aq
objcopy --add-gnu-debuglink=aq.debug aq

install aq ~/bin/
install aq /var/www/utils/aq/aq

install aq.debug ~/bin/
install aq.debug /var/www/utils/aq/aq.debug
