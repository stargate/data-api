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

HCDTAG="1.2.3"
#HCDIMAGE="cr.dtsx.io/datastax/hcd"
HCDIMAGE="559669398656.dkr.ecr.us-west-2.amazonaws.com/engops-shared/hcd/prod/hcd"
HCDONLY="false"
HCDNODES=1

while getopts "dlqn:r:j:" opt; do
  case $opt in
    l)
      DATAAPITAG="v$(../mvnw -f .. help:evaluate -Dexpression=project.version -q -DforceStdout)"
      ;;
    j)
      DATAAPITAG=$OPTARG
      ;;
    n)
      HCDNODES=$OPTARG
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
      echo "  -n - number of HCD nodes to use (1, 2 or 3)"
      echo "  -d - Start only HCD container"
      echo "  -q - enable request logging for APIs in 'io.quarkus.http.access-log' (default: disabled)"
      echo "  -r - specify root log level for APIs (defaults to INFO); usually DEBUG, WARN or ERROR"
      exit 1
      ;;
  esac
done

export LOGLEVEL
export REQUESTLOG
export HCDIMAGE
export HCDTAG
export HCDNODES
export DATAAPITAG
export DATAAPIIMAGE

if [ -z "${HCD_PORT}" ]; then
  export HCD_PORT="9042"
fi

if [ -z "${HCD_FWD_PORT}" ]; then
  export HCD_FWD_PORT="9042"
fi

echo "Running with HCD $HCDIMAGE:$HCDTAG ($HCDNODES nodes), Data API $DATAAPIIMAGE:$DATAAPITAG"

if [ "$HCDONLY" = "true" ]; then
  docker compose -f docker-compose-hcd.yml up -d --wait hcd-1
  exit 0
else
  if [ "$HCDNODES" = 1 ]; then
    docker compose -f docker-compose-hcd.yml up -d --wait hcd-1 data-api
  elif [ "$HCDNODES" = 2 ]; then
    docker compose -f docker-compose-hcd.yml up -d --wait hcd-1 hcd-2 data-api
  else
    docker compose -f docker-compose-hcd.yml up -d --wait
  fi
fi
