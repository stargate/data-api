# 3 — Start everything (HCD, OpenSearch, Data API)

Starts the local stack with Podman: HCD, OpenSearch, the Data API (`data-api-os`, OpenSearch feature flag enabled), Prometheus and Grafana. Run each section in order.

Next: [4-Test HCD and OpenSearch.md](4-Test%20HCD%20and%20OpenSearch.md).

---

## Prerequisites

- **Podman** and **podman-compose** — [Install guide](https://podman.io/docs/installation)
- **HCD image** — download the HCD image from the private IBM repositories (access and download steps are not disclosed here) so that it is available in your local Podman image store.
- **Data API image** — build the `data-api-os` image by following [2-Build data-api image.md](2-Build%20data-api%20image.md). By default the stack uses `localhost/data-api-os:latest`.

### Verify the images are available (before starting)

```bash
podman images data-api-os
podman images --filter reference='*hcd*'
```

Expected: `localhost/data-api-os:latest` and the HCD image are both listed. If `data-api-os` is missing, build it first (see the prerequisite above); otherwise the `data-api` container will fail to start.

### Start the stack and verify containers

```bash
./start.sh
```

Wait ~2 minutes for HCD and OpenSearch to become healthy, then confirm:

```bash
podman ps --format 'table {{.Names}}\t{{.Status}}'
```

Expected output — all five rows should show `healthy` or `Up`:

```
NAMES       STATUS
opensearch  Up ... (healthy)
hcd         Up ... (healthy)
data-api    Up ... (healthy)
prom        Up ...
grafana     Up ...
```

Confirm the running Data API container uses the expected image and is healthy with the OpenSearch feature flag enabled:

```bash
podman inspect data-api --format '{{.ImageName}}'
podman exec data-api printenv JAVA_OPTS_APPEND
curl -s http://localhost:8181/stargate/health
```

Expected: the image is `localhost/data-api-os:latest`, `JAVA_OPTS_APPEND` contains `-Dstargate.feature.flags.open-search=true`, and the health endpoint returns a status of `UP`.

> The OpenSearch flag is set as a system property, not as `STARGATE_FEATURE_FLAGS_OPEN_SEARCH`: that environment variable makes the Data API fail at startup.

