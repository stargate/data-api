#!/bin/bash

# Run cqlsh directly inside the hcd container — avoids all network resolution issues
if ! podman inspect hcd &>/dev/null; then
  echo "Error: could not find a running 'hcd' container."
  exit 1
fi

echo "Connecting to hcd via exec..."
podman exec -it hcd /opt/hcd/bin/cqlsh -u cassandra -p cassandra
