#!/bin/sh

# Check for anonymous field stuct constructors:
# grep -e '{\( \+\)\?[.a-zA-Z]\+,' -RIn ./internal ./cmd ./public
# pcregrep -Mnr '{([\n \t]+)?([.a-zA-Z]+,( )?)+$' ./internal ./cmd ./public

go install golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@latest
fieldalignment -fix ./internal/... ./cmd/... ./public/...
