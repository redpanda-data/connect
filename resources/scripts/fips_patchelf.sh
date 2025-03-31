#!/bin/sh

test -z "$1" && echo "usage: $0 <path/to/binary>" && exit

patchelf --set-interpreter "${PREFIX:=/opt/redpanda/rpk-fips}/lib/ld.so" $1