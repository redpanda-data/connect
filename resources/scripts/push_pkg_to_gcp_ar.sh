#!/usr/bin/env bash

# Push a rpm or deb to GCP Artifact Registry

set -ex

PKG_FILE=$1
PKG_VERSION=$2

if [[ "$PKG_FILE" == "" ]]; then
  echo "Usage: $0 <pkg_file> <pkg_version>"
  exit 1
fi

if [[ "$PKG_FILE" == *.rpm ]]; then
  PKG_TYPE="rpm"
elif [[ "$PKG_FILE" == *.deb ]]; then
  PKG_TYPE="deb"
else
  echo "Unknown package type: $PKG_FILE"
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
  version=$(echo "$PKG_VERSION" | cut -c2-)
else
  version=$PKG_VERSION
fi

GA_VERSION_PATTERN='^[0-9]+\.[0-9]+\.[0-9]+$'
if [[ $version =~ $GA_VERSION_PATTERN ]]; then
  repo="redpanda"
else
  repo="redpanda-unstable"
fi

if [[ "$PKG_TYPE" == "deb" ]]; then
  gcloud artifacts apt upload "${repo}-apt" --location=us-central1 --source="$PKG_FILE" --project=production-devprod
else
  gcloud artifacts yum upload "${repo}-yum" --location=us-central1 --source="$PKG_FILE" --project=production-devprod
fi
