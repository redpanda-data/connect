#!/usr/bin/env bash

# Push a rpm or deb to Cloudsmith

set -ex

PKG_FILE=$1
PKG_VERSION=$2

if [[ "$PKG_FILE" == "" ]]; then
    echo "Usage: $0 <pkg_file> <pkg_version>"
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

if [[ -z $PKG_VERSION ]]; then
    echo "Usage: $0 <pkg_file> <pkg_version>"
    exit 1
fi

# goreleaser removes `v` in front of the {{.Version}}
# the check for release repos should be agnostic of
# the existence of `v`
if [[ $PKG_VERSION == v* ]]; then 
    version=$(echo $PKG_VERSION | cut -c2-)
else
    version=$PKG_VERSION
fi

GA_VERSION_PATTERN='^\d+\.\d+\.\d+$'
if [[ $version =~ $GA_VERSION_PATTERN ]]; then
  repo="redpanda"
else
  repo="redpanda-unstable"
fi

cloudsmith push "$PKG_TYPE" redpanda/$repo/any-distro/any-version "$PKG_FILE" --republish
