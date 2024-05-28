#!/usr/bin/env bash

set -eux

_OS=$1
_PATH_TO_SIGN=$2
_IS_SNAPSHOT=$3


if [ "$_OS" = "darwin" ]; then
  quill sign-and-notarize "$_PATH_TO_SIGN" --dry-run="$_IS_SNAPSHOT" --ad-hoc="$_IS_SNAPSHOT" -vv
else
  echo "No need to sign binaries for ${_OS}"
fi
