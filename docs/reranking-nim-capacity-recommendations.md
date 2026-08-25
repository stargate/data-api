# Reranking NIM capacity recommendations (GPU plane)

Companion to the Data API reranking concurrency gate (`max-concurrent-batches` /
`max-concurrent-calls` / `max-queued-calls` / `total-timeout-millis`, see
[CONFIGURATION.md](../CONFIGURATION.md#reranking-configuration)). The gate makes a burst degrade
gracefully on the Data API side; this document covers the `gpu-helm-charts` / NIM side changes that
give the burst somewhere to go. Target scenario: PepsiCo's requirement of **1000 simultaneous
`findAndRerank` requests with zero errors**.

## Why the model change is the main lever

Queuing converts errors into latency; it does not create throughput. The measured drain rate of a
single `llama-3.2-nv-rerankqa-1b-v2` pod (NIM 1.3.1.0, 10-passage batches) is ~12–16 calls/s, and
each `findAndRerank` fans out ~10 batch calls, so a 1000-request burst is ~10,000 calls — minutes
of queue at any realistic replica count.

`llama-3.2-nemoretriever-500m-rerank-v2` (NIM 2.x runtime) accepts **512 passages per call**, so a
typical `findAndRerank` becomes **one** reranking call. PepsiCo's own benchmark (their infra,
nemotron v2 on NIM 2.0.0, 2 pods x 1 engine) absorbed an 1800-request burst with 0% errors at
~25 calls/s aggregate. That is the existence proof for the 1000-burst target on a small deployment.

## Chart changes needed (gpu-helm-charts)

1. **Deploy the nemotron rerank NIM**: `nemo-reranking` currently ships only
   `llama-3.2-nv-rerankqa-1b-v2` at image tag `1.3.1.0`. Add a
   `llama-3.2-nemoretriever-500m-rerank-v2` service on the NIM 2.x runtime.
2. **Ingress route**: add the per-model route the Data API config points at:
   `/nvidia/v1/ranking/nvidia-llama-3-2-nemoretriever-500m-rerank-v2` →
   `nvidia-llama-3-2-nemoretriever-500m-rerank-v2.reranking-nim.svc.cluster.local` (same pattern as
   the existing `nvidia-llama-3-2-nv-rerankqa-1b-v2` route in `ingress/chart/templates/config.yaml`).
   The default `/nvidia/v1/ranking` route stays on the legacy model until cutover.
3. **NIM queue/shedding env vars** (2.x runtime only — the 1.x runtime does not expose them):
   - `NIM_SERVER_REQUEST_TIMEOUT_S`: set **at or below** the Data API `total-timeout-millis`
     (default 30s → set 30). The default is 120s, which makes the NIM serve answers nobody is
     waiting for once the Data API deadline has fired.
   - `NIM_SERVER_MAX_QUEUE_SIZE`: cap the NIM-side queue so overload sheds (503) instead of
     queueing unboundedly. Size it to at least the worst-case aggregate in-flight from the Data
     API: `data_api_pods x max-concurrent-calls / nim_replicas` (e.g. 4 Data API pods x 32 / 2
     replicas = 64), plus headroom. With the Data API gate in place the NIM queue should rarely
     grow beyond that — the Data API queue does the shedding with a client-friendly error instead.

## Replica sizing

```
replicas >= burst_calls / (per_pod_drain_rate x deadline_seconds)
```

Worked example for the 1000-burst target with 512-passage calls, ~12.5 calls/s per pod (PepsiCo's
measured aggregate ~25/s on 2 pods) and the 30s Data API deadline:

```
replicas >= 1000 / (12.5 x 30) ≈ 2.7  →  3 replicas (2 minimum + headroom)
```

**Measure `per_pod_drain_rate` on our own L40S (g6e-xlarge) nodes before committing** — the
PepsiCo figure comes from their hardware, and 512-passage calls have very different per-call cost
than 10-passage calls. The `retrieval-eval/tools/rerank-loadtest` tool in burst mode
(`--iterations-per-user 1`) reproduces the scenario; Triton queue depth is visible only in the
`nv_inference_*` metrics on port 8002.

## Interplay with the Data API gate (deploy order)

1. Data API ships first: gate defaults (32 in-flight/pod, queue 1000, 30s deadline) are safe
   against the existing NIM.
2. NIM 2.x model + ingress route next (this document).
3. Tune `NIM_SERVER_*` and replica count from the load-test measurements.

Under a 1000-burst with 4 Data API pods, at most 128 calls hit the GPU plane simultaneously; the
rest wait in the Data API queues and either get served within their deadline or fail fast with
`RERANKING_PROVIDER_OVERLOADED` / `RERANKING_PROVIDER_TIMEOUT` — the queue depth, wait time, and
rejection metrics (`rerank.all.queue.*`) show which regime the system is in.
