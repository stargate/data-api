#!/bin/sh

# Default to INFO as root log level
LOGLEVEL=INFO

# Default to images used in project integration tests
DSETAG="$(../mvnw -f .. help:evaluate -Dexpression=stargate.int-test.cassandra.image-tag -q -DforceStdout)"
SGTAG="$(../mvnw -f .. help:evaluate -Dexpression=stargate.int-test.coordinator.image-tag -q -DforceStdout)"
JSONTAG="v$(../mvnw -f .. help:evaluate -Dexpression=project.version -q -DforceStdout)"

while getopts "qr:t:j:" opt; do
  case $opt in
    q)
      REQUESTLOG=true
      ;;
    r)
      LOGLEVEL=$OPTARG
      ;;
    t)
      SGTAG=$OPTARG
      ;;
    j)
      JSONTAG=$OPTARG
      ;;
    \?)
      echo "Valid options:"
      echo "  -t <tag> - use Docker images tagged with specified Stargate version (will pull images from Docker Hub if needed)"
      echo "  -j <tag> - use Docker images tagged with specified JSON API version (will pull images from Docker Hub if needed)"
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
export JSONTAG

echo "Running with DSE $DSETAG, Stargate $SGTAG, JSON API $JSONTAG"

docker-compose -f docker-compose-dev-mode.yml up -d
