#!/bin/bash

# require Docker Compose v2
if [[ ! $(docker compose version --short) =~ ^2. ]]; then
  echo "Docker compose v2 required. Please upgrade Docker Desktop to the latest version."
  exit 1
fi

# Default to INFO as root log level
LOGLEVEL=INFO

# Get the latest DSE7 image tag from Docker Hub
DSE7TAG="$(curl -s 'https://registry.hub.docker.com/v2/repositories/datastax/dse-server/tags/?page_size=1000' | jq -r '[.results[] | select(.name | startswith("7.0.0"))] | sort_by(.last_updated) | last | .name')"

# Default to latest released version
JSONTAG="v1"
JSONIMAGE="stargateio/jsonapi"

while getopts "lqnr:t:j:" opt; do
  case $opt in
    l)
      JSONTAG="v$(../mvnw -f .. help:evaluate -Dexpression=project.version -q -DforceStdout)"
      ;;
    j)
      JSONTAG=$OPTARG
      ;;
    n)
      JSONIMAGE="stargateio/jsonapi-native"
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
      echo "  -l - use JSON API Docker image from local build (see project README for build instructions)"
      echo "  -j <tag> - use JSON API Docker image tagged with specified JSON API version (will pull images from Docker Hub if needed)"
      echo "  -n <tag> - use JSON API native image instead of default Java-based image"
      echo "  -t <tag> - use Stargate coordinator Docker image tagged with specified  version (will pull images from Docker Hub if needed)"
      echo "  -q - enable request logging for APIs in 'io.quarkus.http.access-log' (default: disabled)"
      echo "  -r - specify root log level for APIs (defaults to INFO); usually DEBUG, WARN or ERROR"
      exit 1
      ;;
  esac
done

export LOGLEVEL
export REQUESTLOG
export DSE7TAG
export SGTAG
export JSONTAG
export JSONIMAGE

echo "Running with DSE $DSE7TAG, JSON API $JSONIMAGE:$JSONTAG"

docker compose -f docker-compose-dse7.yml up -d --wait