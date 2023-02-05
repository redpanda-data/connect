#!/bin/sh

go install github.com/jondot/goweight@latest
goweight ./cmd/benthos
