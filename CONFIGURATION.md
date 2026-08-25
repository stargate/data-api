# Configuration Guide

## Quarkus Configuration

The complete list of Quarkus available properties can be found on [All configuration options](https://quarkus.io/guides/all-config) page.

Other Quarkus properties that are specifically relevant for the service:

* `quarkus.http.limits.max-body-size` - maximum HTTP payload size (in bytes) that the server will accept. Default is 20MB.

## Database limits configuration
*Configuration for document limits, defined by [DatabaseLimitsConfig.java](src/main/java/io/stargate/sgv2/jsonapi/config/DatabaseLimitsConfig.java).*

| Property                                                  | Type  | Default | Description                                                                                       |
|-----------------------------------------------------------|-------|---------|---------------------------------------------------------------------------------------------------|
| `stargate.database.limits.max-collections`                | `int` | `5`     | The maximum number of Collections allowed to be created per Database.                             |
| `stargate.database.limits.indexes-needed-per-collection`  | `int` | `10`    | Number of indexes needed per Collection (to determine if a new Collection may be added).          |
| `stargate.database.limits.indexes-available-per-database` | `int` | `50`    | Number of indexes assumed to be available per Database (to determine if Collection may be added). |

## Document limits configuration
*Configuration for document limits, defined by [DocumentLimitsConfig.java](src/main/java/io/stargate/sgv2/jsonapi/config/DocumentLimitsConfig.java).*

| Property                                                        | Type  | Default     | Description                                                                             |
|-----------------------------------------------------------------|-------|-------------|-----------------------------------------------------------------------------------------|
| `stargate.jsonapi.document.limits.max-size`                     | `int` | `4_000_000` | The maximum size of (in characters) a single document.                                  |
| `stargate.jsonapi.document.limits.max-depth`                    | `int` | `16`        | The maximum document depth (nesting).                                                   |
| `stargate.jsonapi.document.limits.max-property-path-length`     | `int` | `1000`       | The maximum length of property paths in a document (segments and separating periods)    |
| `stargate.jsonapi.document.limits.max-object-properties`        | `int` | `1000`      | The maximum number of properties any single indexable object in a document can contain. |
| `stargate.jsonapi.document.limits.max-document-properties`      | `int` | `2000`      | The maximum number of total indexed properties a document can contain.                        |
| `stargate.jsonapi.document.limits.max-number-length`            | `int` | `100`       | The maximum length (in characters) of a single number value in a document.              |
| `stargate.jsonapi.document.limits.max-string-length-in-bytes`   | `int` | `8000`      | The maximum length (in bytes) of a single indexable string value in a document.         |
| `stargate.jsonapi.document.limits.max-array-length`             | `int` | `1000`      | The maximum length (in elements) of a single indexable array in a document.             |
| `stargate.jsonapi.document.limits.max-vector-embedding-length`  | `int` | `4096`      | The maximum length (in floats) of the $vector in a document.                            |

## Operations configuration
*Configuration for the operation execution, defined by [OperationsConfig.java](src/main/java/io/stargate/sgv2/jsonapi/config/OperationsConfig.java).*

| Property                                                                | Type      | Default  | Description                                                                                                                                                                                        |
|-------------------------------------------------------------------------|-----------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `stargate.jsonapi.operations.default-page-size`                         | `int`     | `50`     | The default Cassandra page size used for read queries. Maximum configurable value is `500`.                                                                                                        |
| `stargate.jsonapi.operations.default-sort-page-size`                    | `int`     | `100`    | The default Cassandra page size used for read queries that are used for sorting purposes.                                                                                                          |
| `stargate.jsonapi.operations.max-document-sort-count`                   | `int`     | `10_000` | The maximum amount of documents that could be sorted using the in-memory sorting. The request will fail in case in-memory sorting would break the limit.                                           |
| `stargate.jsonapi.operations.max-document-insert-count`                 | `int`     | `20`     | The maximum amount of documents that can be inserted in a single operation. The request will fail fast without inserts if the limit is broken.                                                     |
| `stargate.jsonapi.operations.max-document-update-count`                 | `int`     | `20`     | The maximum amount of documents that can be updated in a single operation. In case there are more documents that could be updated, the operation will set the `moreData` response status to `true`. |
| `stargate.jsonapi.operations.max-document-delete-count`                 | `int`     | `20`     | The maximum amount of documents that can be deleted in a single operation. In case there are more documents that could be deleted, the operation will set the `moreData` response status to `true`. |
| `stargate.jsonapi.operations.max-filter-object-properties`              | `int`     | `64`     | The maximum number of properties a single filter clause can contain.                                                                                                                               |
| `stargate.jsonapi.operations.max-in-operator-value-size`                | `int`     | `100`    | The maximum number of _id values that can be passed for `$in` operator.                                                                                                                            |
| `stargate.jsonapi.operations.lwt.retries`                               | `int`     | `3`      | The amount of client side retries in case of a LWT failure.                                                                                                                                        |
| `stargate.jsonapi.operations.database-config.session-cache-ttl-seconds` | `int`     | `300`    | The amount of seconds that the cql session will be kept in memory after last access.                                                                                                               |
| `stargate.jsonapi.operations.database-config.session-cache-max-size`    | `int`     | `50`     | The maximum number of cql sessions that will be kept in memory.                                                                                                                                    |
| `stargate.jsonapi.operations.default-count-page-size`                   | `int`     | `100`    | The default Cassandra page size used for reading keys for count command.                                                                                                                           |
| `stargate.jsonapi.operations.max-count-limit`                           | `int`     | `1000`   | The default maximum number of rows to read for count operation.                                                                                                                                    |
| `stargate.jsonapi.operations.database-config.ddl-retry-delay-millis`    | `int`     | `1000`   | Delay time in seconds for DDL timeout.                                                                                                                                                             |
| `stargate.jsonapi.operations.database-config.ddl-delay-millis`          | `int`     | `2000`   | Delay between create table and create index to get the schema sync.                                                                                                                                |
| `stargate.jsonapi.operations.vectorize-enabled`                         | `boolean` | `false`  | Flag to enable server side vectorization.                                                                                                                                              |


## Jsonapi metering configuration
*Configuration for jsonapi metering, defined by [JsonApiMetricsConfig.java](io/stargate/sgv2/jsonapi/api/v1/metrics/JsonApiMetricsConfig.java).*

| Property                              | Type     | Default       | Description                                                  |
|---------------------------------------|----------|---------------|--------------------------------------------------------------|
| `stargate.jsonapi.metric.error-class` | `string` | `error.class` | Metrics tag that provides information about the error class. |
| `stargate.jsonapi.metric.error-code`  | `string` | `error.code`  | Metrics tag that provides information about the error code.  |
| `stargate.jsonapi.metric.command`     | `string` | `command`     | Metrics tag that provides information about the command.     |
| `stargate.jsonapi.metric.metrics.name`| `string` | `jsonapi`     | Metrics name prefix.                                         |


## Command level logging configuration
*Configuration for command level logging, defined by [CommandLevelLoggingConfig.java](src/main/java/io/stargate/sgv2/jsonapi/config/CommandLoggingConfig.java).*

| Property                                            | Type      | Default | Description                                                                                                                                                        |
|-----------------------------------------------------|-----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `stargate.jsonapi.logging.enabled`                  | `boolean` | `false` | Setting it to `true` enables command level logging.                                                                                                                |
| `stargate.jsonapi.logging.only-results-with-errors` | `boolean` | `true`  | Setting it to `true` prints the command level info only for the commands where the command result has errors.                                                      |
| `stargate.jsonapi.logging.enabled-tenants`          | `string`  | `ALL`   | Comma separated list of tenants for which command level logging should be enabled. Default is a special keyword called `ALL` which prints this log for all tenants |

## API Feature enabling configuration
*Configuration for enabling Features, defined by [FeaturesConfig.java](src/main/java/io/stargate/sgv2/jsonapi/config/CommandLoggingConfig.java).*

| Property                        | Type      | Default | Description                                                                                                         |
|---------------------------------|-----------|---------|---------------------------------------------------------------------------------------------------------------------|
| `stargate.feature.flags.tables` | `boolean` | `true` (enabled by default)   | Setting it to `true` enables Tables functionality; `false` disables; leaving as `null` uses the default (enabled).|

## Reranking configuration
*Configuration for reranking providers and the reranking concurrency gate, defined by [RerankingProvidersConfig.java](src/main/java/io/stargate/sgv2/jsonapi/service/reranking/configuration/RerankingProvidersConfig.java). Providers and models are loaded from `reranking-providers-config.yaml` (overridable with the `RERANKING_CONFIG_PATH` env var or the `RERANKING_CONFIG_RESOURCE` system property); when the embedding gateway is enabled the provider/model list is served by the gateway instead.*

Each model under `stargate.jsonapi.reranking.providers.<provider>.models[]` supports the following `properties`:

| Property                 | Type     | Default | Description                                                                                                                                                                                                            |
|--------------------------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `at-most-retries`        | `int`    | `3`     | Maximum attempts per batch call before failing (1 request + 2 retries).                                                                                                                                                |
| `initial-back-off-millis`| `int`    | `100`   | Initial retry delay, doubling up to `max-back-off-millis`.                                                                                                                                                             |
| `read-timeout-millis`    | `int`    | `5000`  | HTTP read timeout for a single batch call.                                                                                                                                                                             |
| `max-back-off-millis`    | `int`    | `500`   | Maximum retry delay.                                                                                                                                                                                                   |
| `jitter`                 | `double` | `0.5`   | Random variation applied to retry delays.                                                                                                                                                                              |
| `max-batch-size`         | `int`    | (none)  | Maximum passages per reranking call; a rerank request fans out into `ceil(passages / max-batch-size)` calls.                                                                                                            |
| `max-concurrent-batches` | `int`    | `8`     | Per-request fan-out cap: how many batch calls a single rerank request runs concurrently.                                                                                                                                |
| `max-concurrent-calls`   | `int`    | `32`    | Per-process bulkhead: reranking calls in flight at once across all requests for this model. Excess calls wait in a FIFO queue.                                                                                          |
| `max-queued-calls`       | `int`    | `1000`  | Bounded FIFO queue depth for calls waiting on `max-concurrent-calls`. When full, further calls fail immediately with `RERANKING_PROVIDER_OVERLOADED` without calling the provider. `0` disables queueing (fail fast).   |
| `total-timeout-millis`   | `int`    | `30000` | Overall deadline for one rerank request, covering queue wait, all batch calls, and retries. Must be at least `read-timeout-millis`. On expiry the request fails with `RERANKING_PROVIDER_TIMEOUT` and parked work is abandoned without ever calling the provider. |

Sizing rule of thumb: a queue entry only makes sense if it can be served within the deadline, so keep `max-queued-calls` at or below `max-concurrent-calls x (total-timeout-millis / typical-call-latency-millis)`; entries beyond that will burn their whole deadline waiting and still fail.

The gate exposes Micrometer metrics per provider+model: `rerank.all.queue.depth` and `rerank.all.inflight.calls` (gauges), `rerank.all.queue.wait.duration` (timer, recorded for calls that actually waited), and `rerank.all.queue.rejected.count` (counter).
