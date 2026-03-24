#!/usr/bin/env bash

TOPIC="${1:-oracle-cdc.TESTDB.PRODUCTS}"
INTERVAL="${2:-1}"

prev=0
while true; do
    current=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
        --bootstrap-server=localhost:9092 \
        --topic="$TOPIC" 2>/dev/null \
        | awk -F: '{sum += $3} END {print sum+0}')

    echo "$(date +%H:%M:%S)  msgs/s: $((current - prev))  total: $current"
    prev=$current
    sleep "$INTERVAL"
done
