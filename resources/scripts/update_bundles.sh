#!/usr/bin/env bash

# This script should be run from the root of the repository.
#
# Iterates each bundle we provide for Redpanda Connect plugins (enterprise,
# community, etc) and upgrades all dependencies (go get -u).

for dir in $(ls ./public/bundle); do
    ( cd "./public/bundle/$dir" && go get -u . && go mod tidy )
done

