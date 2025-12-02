#!/bin/bash
set -e
[ -f .env.validation ] || exit 1
set -a; source .env.validation; set +a

for f in *.yaml; do
    rpk connect lint "$f" >/dev/null 2>&1 || {
        echo "❌ $f" >&2
        rpk connect lint "$f" 2>&1 | sed 's/^/   /' >&2
        exit 1
    }
done
