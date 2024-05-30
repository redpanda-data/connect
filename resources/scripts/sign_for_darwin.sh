#!/usr/bin/env bash

set -eux

_OS=$1
_PATH_TO_SIGN=$2
_IS_SNAPSHOT=$3

check_cmd() {
	command -v "$1" > /dev/null 2>&1
}

if [ "$_OS" = "darwin" ]; then
  if check_cmd "quill"; then
    quill sign-and-notarize "$_PATH_TO_SIGN" --dry-run="$_IS_SNAPSHOT" --ad-hoc="$_IS_SNAPSHOT" -vv
  else
    echo "Aborted, missing quill"
  fi
else
  echo "No need to sign binaries for ${_OS}"
fi
