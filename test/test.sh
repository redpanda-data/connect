#!/bin/bash
echo -e "test1\np1\np2\n\ntest2\np1\np2\n\ntest3\n" | benthos -c ./test/stdout.yaml
