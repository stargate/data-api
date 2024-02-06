#!/bin/bash

# require Docker Compose v2
if [[ ! $(docker compose version --short) =~ ^2. ]]; then
  echo "Docker compose v2 required. Please upgrade Docker Desktop to the latest version."
  exit 1
fi

# Default to INFO as root log level
LOGLEVEL=INFO

# Default to images used in project integration tests
DSETAG="$(../mvnw -f .. help:evaluate -Dexpression=stargate.int-test.cassandra.image-tag -q -DforceStdout)"
SGTAG="$(../mvnw -f .. help:evaluate -Dexpression=stargate.int-test.coordinator.image-tag -q -DforceStdout)"

# Default to latest released version
DATAAPITAG="v1"
DATAAPIIMAGE="stargateio/jsonapi"

while getopts "lqnr:t:j:" opt; do
  case $opt in
    l)
      DATAAPITAG="v$(../mvnw -f .. help:evaluate -Dexpression=project.version -q -DforceStdout)"
      ;;
    j)
      DATAAPITAG=$OPTARG
      ;;
    n)
      DATAAPIIMAGE="stargateio/jsonapi-native"
      ;;
    q)
      REQUESTLOG=true
      ;;
    r)
      LOGLEVEL=$OPTARG
      ;;
    t)
      SGTAG=$OPTARG
      ;;
    \?)
      echo "Valid options:"
      echo "  -l - use DATA API Docker image from local build (see project README for build instructions)"
      echo "  -j <tag> - use DATA API Docker image tagged with specified DATA API version (will pull images from Docker Hub if needed)"
      echo "  -n <tag> - use DATA API native image instead of default Java-based image"
      echo "  -t <tag> - use Stargate coordinator Docker image tagged with specified  version (will pull images from Docker Hub if needed)"
      echo "  -q - enable request logging for APIs in 'io.quarkus.http.access-log' (default: disabled)"
      echo "  -r - specify root log level for APIs (defaults to INFO); usually DEBUG, WARN or ERROR"
      exit 1
      ;;
  esac
done

export LOGLEVEL
export REQUESTLOG
export DSETAG
export SGTAG
export DATAAPITAG
export DATAAPIIMAGE

echo "Running with DSE $DSETAG, Stargate $SGTAG, DATA API $DATAAPIIMAGE:$DATAAPITAG"

docker compose up -d --wait
