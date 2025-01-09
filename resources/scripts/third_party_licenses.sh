#!/usr/bin/env bash

# This script should be run from the root of the repository.
#
# Creates a summary of all third party dependencies and their licenses.
#
# This script requires `go-licenses` to be installed:
#
# go install github.com/google/go-licenses@latest

go-licenses report github.com/redpanda-data/connect/v4/cmd/redpanda-connect \
    --template ./resources/scripts/third_party.md.tpl \
    > licenses/third_party.md

