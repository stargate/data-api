#!/bin/bash

# require Docker Compose v2
if [[ ! $(docker compose version --short) =~ ^2. ]]; then
  echo "Docker compose v2 required. Please upgrade Docker Desktop to the latest version."
  exit 1
fi

# Default to INFO as root log level
LOGLEVEL=INFO

# Default to images used in project integration tests
HCDTAG="$(../mvnw -f .. help:evaluate -Phcd-it -Dexpression=stargate.int-test.cassandra.image-tag -q -DforceStdout)"

# Default to latest released version
DATAAPITAG="v1"
DATAAPIIMAGE="stargateio/data-api"
HCDONLY="false"

while getopts "dlqnr:j:" opt; do
  case $opt in
    l)
      DATAAPITAG="v$(../mvnw -f .. help:evaluate -Dexpression=project.version -q -DforceStdout)"
      ;;
    j)
      DATAAPITAG=$OPTARG
      ;;
    n)
      DATAAPIIMAGE="stargateio/data-api-native"
      ;;
    q)
      REQUESTLOG="true"
      ;;
    r)
      LOGLEVEL=$OPTARG
      ;;
    d)
      HCDONLY="true"
      ;;
    \?)
      echo "Valid options:"
      echo "  -l - use Data API Docker image from local build (see project README for build instructions)"
      echo "  -j <tag> - use Data API Docker image tagged with specified Data API version (will pull images from Docker Hub if needed)"
      echo "  -n <tag> - use Data API native image instead of default Java-based image"
      echo "  -d - Start only HCD container"
      echo "  -q - enable request logging for APIs in 'io.quarkus.http.access-log' (default: disabled)"
      echo "  -r - specify root log level for APIs (defaults to INFO); usually DEBUG, WARN or ERROR"
      exit 1
      ;;
  esac
done

export LOGLEVEL
export REQUESTLOG
export HCDTAG
export DATAAPITAG
export DATAAPIIMAGE

echo "Running with DSE $DSETAG, Data API $DATAAPIIMAGE:$DATAAPITAG"

if [ "$HCDONLY" = "true" ]; then
  docker compose -f docker-compose-hcd.yml up -d --wait hcd
  exit 0
else
  docker compose -f docker-compose-hcd.yml up -d --wait
fi
