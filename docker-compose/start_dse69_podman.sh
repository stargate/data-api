#!/bin/bash

source .env

# Check podman version is >= 5
PODMAN_VERSION=$(podman --version | awk '{print $3}')
PODMAN_COMPOSE_VERSION=$(podman-compose version | awk '{print $3}')

# Function to compare semantic versions
version_greater_equal() {
  [ "$(printf '%s\n' "$2" "$1" | sort -V | head -n1)" = "$2" ]
}

if ! version_greater_equal "$PODMAN_VERSION" "5.0.0"; then
  echo "Podman >= 5.0.0 is required. Found: $PODMAN_VERSION"
  exit 1
fi

if ! version_greater_equal "$PODMAN_COMPOSE_VERSION" "1.4.0"; then
  echo "Podman Compose >= 1.4.0 is required. Found: $PODMAN_COMPOSE_VERSION"
  exit 1
fi

# Default to INFO as root log level
LOGLEVEL=INFO

# Default to latest released version
DATAAPITAG="v1"
DATAAPIIMAGE="stargateio/data-api"

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
  podman-compose -f docker-compose.yml up -d dse-1

  # Wait for dse-1 to become healthy
  until podman inspect --format '{{.State.Health.Status}}' dse-1 2>/dev/null | grep -q healthy; do
    echo "Waiting for dse-1 to become healthy..."
    sleep 2
  done
  echo "dse-1 is healthy."
  exit 0
else
  if [ "$DSENODES" = "1" ]; then
    podman-compose -f docker-compose.yml up -d dse-1 data-api
  elif [ "$DSENODES" = "2" ]; then
    podman-compose -f docker-compose.yml up -d dse-1 dse-2 data-api
  else
    podman-compose -f docker-compose.yml up -d
  fi
fi
