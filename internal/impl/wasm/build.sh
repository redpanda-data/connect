#!/bin/sh
tinygo build -scheduler=none -target=wasi -o uppercase.wasm ../../../public/wasm/examples/tinygo
