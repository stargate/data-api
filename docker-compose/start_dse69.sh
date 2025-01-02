#!/bin/bash

# require Docker Compose v2
if [[ ! $(docker compose version --short) =~ ^2. ]]; then
  echo "Docker compose v2 required. Please upgrade Docker Desktop to the latest version."
  exit 1
fi

# Default to INFO as root log level
LOGLEVEL=INFO

# Default to latest released version
DATAAPITAG="v1"
DATAAPIIMAGE="stargateio/data-api"

DSETAG="6.9.5"
DSEIMAGE="cr.dtsx.io/datastax/dse-server"
DSEONLY="false"
DSENODES=1

while getopts "dlqn:r:j:" opt; do
  case $opt in
    l)
      DATAAPITAG="v$(../mvnw -f .. help:evaluate -Dexpression=project.version -q -DforceStdout)"
      ;;
    j)
      DATAAPITAG=$OPTARG
      ;;
    n)
      DSENODES=$OPTARG
      ;;
    q)
      REQUESTLOG="true"
      ;;
    r)
      LOGLEVEL=$OPTARG
      ;;
    d)
      DSEONLY=true
      ;;
    \?)
      echo "Valid options:"
      echo "  -l - use Data API Docker image from local build (see project README for build instructions)"
      echo "  -j <tag> - use Data API Docker image tagged with specified Data API version (will pull images from Docker Hub if needed)"
      echo "  -n - number of DSE nodes to use (1, 2 or 3)"
      echo "  -d - Start only DSE container"
      echo "  -q - enable request logging for APIs in 'io.quarkus.http.access-log' (default: disabled)"
      echo "  -r - specify root log level for APIs (defaults to INFO); usually DEBUG, WARN or ERROR"
      exit 1
      ;;
  esac
done

export LOGLEVEL
export REQUESTLOG
export DSETAG
export DATAAPITAG
export DATAAPIIMAGE
export DSEONLY
export DSENODES

if [ -z "${DSE_PORT}" ]; then
  export DSE_PORT="9042"
fi

if [ -z "${DSE_FWD_PORT}" ]; then
  export DSE_FWD_PORT="9042"
fi

echo "Running with DSE $DSETAG ($DSENODES nodes), Data API $DATAAPIIMAGE:$DATAAPITAG"

if [ "$DSEONLY" = "true" ]; then
  docker compose up -d --wait dse-1
  exit 0
else
  if [ "$DSENODES" = 1 ]; then
    docker compose up -d --wait dse-1 data-api
  elif [ "$DSENODES" = 2 ]; then 
    docker compose up -d --wait dse-1 dse-2 data-api
  else
    docker compose up -d --wait
  fi
fi
