#!/usr/bin/env bash

# This script should be run from the root of the repository.
#
# Creates a new tag for each bundle we provide for Redpanda Connect plugins,
# where the tag matches the pattern public/bundle/<BUNDLE>/<CVER>, where
# <BUNDLE> is the bundle name and <CVER> matches the version of RPCN that the
# bundle references.

for dir in $(ls ./public/bundle); do
    bundle_path="public/bundle/$dir"
    modline=$( cd $bundle_path && cat go.mod | grep "redpanda-data/connect/v" )
    modline_split=( $modline )
    version=${modline_split[2]}
    git tag "$bundle_path/$version"
done

