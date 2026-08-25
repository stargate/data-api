# Reranking concurrency gate: embedding gateway (EGW) coordination

On EGW-enabled deployments (Astra), `RerankingProviderConfigProducer` sources the reranking
provider/model configuration from the gateway's `getSupportedRerankingProviders` gRPC response —
**not** from this repo's `reranking-providers-config.yaml`. Two EGW-side changes are needed for the
concurrency-gate work to be fully effective on Astra.

## 1. Mirror the new proto fields

`embedding_gateway.proto`, message
`GetSupportedRerankingProvidersResponse.ProviderConfig.ModelConfig.RequestProperties`, gained four
fields (7–10, all `optional int32` — `optional` so the Data API can distinguish "not served" from
zero):

```proto
optional int32 max_concurrent_batches = 7;   // per-request fan-out cap        (Data API default 8)
optional int32 max_concurrent_calls  = 8;    // per-pod in-flight bulkhead     (Data API default 32)
optional int32 max_queued_calls      = 9;    // bounded FIFO wait queue        (Data API default 1000)
optional int32 total_timeout_millis  = 10;   // overall per-request deadline   (Data API default 30000)
```

The EGW repo must copy these fields into its own proto and populate them from its model config.

**Fallback semantics on the Data API side** (already shipped, so deploy order is safe): a field
that is unset, or set below its minimum (1 for fields 7/8/10; 0 for `max_queued_calls`, where 0 is
a valid explicit "no queueing"), falls back to the Data API default listed above. An old gateway
therefore yields safe defaults — never zero permits.

## 2. Serve the nemotron model with max_batch_size 512

The Data API yaml adds `nvidia/llama-3.2-nemoretriever-500m-rerank-v2` with `max-batch-size: 512`,
but on Astra the served model list comes from EGW. Until EGW adds the same model entry:

- Astra tenants cannot select the nemotron model at all, and
- once EGW adds it, `max_batch_size` **must be 512** there. If EGW serves a smaller value the
  fan-out collapse (10 calls → 1 call per findAndRerank) — the main throughput lever for the
  PepsiCo 1000-burst target — silently does not happen on Astra.

The gate concurrency fields can initially be left unserved (Data API defaults apply); serving them
from EGW is what makes them tunable per environment without a Data API release.

## Timeout interaction note

On the EGW path the Data API's `read-timeout-millis` is not applied (the gRPC deadline is
EGW-side). The new `total_timeout_millis` deadline IS applied on the EGW path — it wraps the whole
rerank request in the Data API — so make sure EGW's own upstream timeout for reranking calls is at
or below it, or EGW will keep working on requests the Data API has already failed.
