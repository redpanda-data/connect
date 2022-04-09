#!/bin/sh

set -euf

if [ $# -eq 0 ]; then
  echo "Usage: $0 <path_to_benthos_binary>"
  exit 1
fi

benthosbin="$1"
if [ ! -f "$benthosbin" ]; then
  echo "$benthosbin is not a file"
  exit 1
fi

if ! command -v cue > /dev/null 2>&1; then
    echo "🟡  Skipping cue tests since \`cue\` binary is not available."
    exit 0
fi

basedir="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

$benthosbin list --format cue > "$basedir/benthos.cue"

cd "$basedir"

cue export --out yaml test.cue > actual.yml
if [ "${UPDATE:-0}" -ne "0" ]; then
  cp actual.yml expected.yml
fi

result=0
diff actual.yml expected.yml || result=$?

echo ""

if [ $result -ne 0 ]; then
  echo "🔴  Cue output has changed. If this is intended then rerun this script to update snapshots like so:"
  echo "    UPDATE=1 $0 $*"
else
  echo "🟢  Cue tests passed."
fi
