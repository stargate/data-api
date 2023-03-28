#!/usr/bin/env bash

set -euo pipefail

if [ -z ${1+x} ]; then
   echo "stargate version is a required argument"
   exit 1
fi

STARGATE_VERSION=$1
if [[ "$STARGATE_VERSION" =~ ^v[1-9]\.[0-9]+ ]]; then
   STARGATE_VERSION_NUMBER=$(echo ${STARGATE_VERSION} | cut -c 2-)
else
   echo "stargate version ('$STARGATE_VERSION') must start with prefix 'v' followed by a number, dot, another number (f.ex 'v2.0.9')"
   exit 1
fi

# macOS will not work
if [[ $OSTYPE == 'darwin'* ]]; then
  echo 'You are running macOS, which is not supported by this script'
  echo 'You can automatically bump the Stargate version using the GitHub workflow https://github.com/stargate/jsonapi/actions/workflows/update-parent-version.yaml'
  echo 'If you still want to bump the version manually, then follow the steps below:'
  echo ' 1. Update the version of the sgv2-api-parent parent in the pom.xml'
  echo ' 2. Commit and push your changes'
  exit 1
fi

# update parent version only
sed -i '0,/<\/parent>/ { /<version>/ { s|<version>[^<]*<\/version>|<version>'"$STARGATE_VERSION_NUMBER"'<\/version>|; } }' pom.xml
