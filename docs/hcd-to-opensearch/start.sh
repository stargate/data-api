#!/bin/bash

# Check podman version is >= 5
PODMAN_VERSION=$(podman --version | awk '{print $3}')
PODMAN_COMPOSE_VERSION=$(podman-compose version | awk '{print $3}')

HCD_VERSION=2.0.8
HCD_LOCATION=559669398656.dkr.ecr.us-west-2.amazonaws.com/engops-shared/hcd/qa/hcd
OPENSEARCH_VERSION=3.7.0
PROMETHEUS_VERSION=v3
GRAFANA_VERSION=13.0
MAX_HEAP_SIZE=2G
OPENSEARCH_JAVA_OPTS="-Xms1g -Xmx2g"

export HCD_VERSION
export HCD_LOCATION
export OPENSEARCH_VERSION
export PROMETHEUS_VERSION
export GRAFANA_VERSION
export MAX_HEAP_SIZE
export OPENSEARCH_JAVA_OPTS

export DATAAPITAG=latest
export DATAAPIIMAGE=localhost/data-api-os

podman-compose -f ./docker-compose-data-api-os.yaml up -d
