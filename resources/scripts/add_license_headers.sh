#!/usr/bin/env bash

# This script should be run from the root of the repository.
#
# Scans all files with a .go suffix and filters for files that are missing a
# Copyright notice at the top. Each detected file is then modified to have the
# Apache 2.0 license header at the top, as this is the default license for the
# repository.
#
# Therefore, it is important before running this script that any enterprise
# licensed files are already annotated with the appropriate license header.

tmpFile="./license_script.tmp"

for file in $(find . -name \*.go); do
	topLine=$(head -n 1 $file)
	if [[ $topLine != *"Copyright"* ]]; then
		cat ./licenses/Apache-2.0_header.go.txt > $tmpFile
		cat $file >> $tmpFile
		cat $tmpFile > $file
	fi
done

rm -f $tmpFile
