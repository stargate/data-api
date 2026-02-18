#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/nginx-quarkus.conf"

echo "Starting Quarkus-LB with configs from '$CONFIG_FILE'..."

nginx -c $CONFIG_FILE
