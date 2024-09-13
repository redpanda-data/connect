#!/usr/bin/env bash

# Push a rpm or deb to Cloudsmith

set -ex

PKG_FILE=$1

if [[ "$PKG_FILE" == "" ]]; then
    echo "Usage: $0 <pkg_file>"
    exit 1
fi

if [[ "$CLOUDSMITH_API_KEY" == "" ]]; then
    echo "CLOUDSMITH_API_KEY is not set"
    exit 1
fi

if [[ "$PKG_FILE" == *.rpm ]]; then
    PKG_TYPE="rpm"
elif [[ "$PKG_FILE" == *.deb ]]; then
    PKG_TYPE="deb"
else
    echo "Unknown package type"
    exit 1
fi

cloudsmith push "$PKG_TYPE" redpanda/redpanda/any-distro/any-version "$PKG_FILE" --republish