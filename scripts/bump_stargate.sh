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

# Note: brackets around version needed for parent (version "range") but not for child modules
./mvnw -B versions:update-parent -DparentVersion="[${STARGATE_VERSION_NUMBER}]" -DgenerateBackupPoms=false -DallowSnapshots=true
