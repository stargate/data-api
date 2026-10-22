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

## MCP logging

Normal MCP logs contain generated request IDs, fixed protocol/command categories, HTTP status,
error counts, logger names and exception class names. They exclude document/query contents,
request/response bodies, client-supplied RPC IDs, credential headers, exception messages and
user-controlled logging context. MCP access URLs are excluded from the standard access log and
replaced with request summaries. Structured billing events keep their existing logging path.

Raw framework traffic logging is disabled. The `mcp-safe-logging` filter also discards that category
if `quarkus.mcp.server.traffic-logging.enabled` is overridden. It is attached to the standard console,
file, syslog and socket handlers. Any added handler or log exporter must apply the same filter
before publishing records. Keep these filters in deployment overrides. The MCP endpoint and its
access-log exclusion are derived together from `quarkus.http.root-path` and
`quarkus.mcp.server.http.root-path`. Changing either path requires no separate logging configuration.
Keep the authentication path configuration aligned with the endpoint. Explicit
`quarkus.http.access-log.exclude-pattern` overrides replace the generated exclusion and must also
exclude the MCP endpoint and its descendants. Ordinary HTTP API logging is unaffected.

For a diagnostic session, set `stargate.jsonapi.mcp.diagnostics-until` (environment variable
`STARGATE_JSONAPI_MCP_DIAGNOSTICS_UNTIL`) to an absolute ISO-8601 UTC timestamp no more than 15 minutes
in the future at startup. Diagnostics add at most 32 stack frames per exception. They expire at
that timestamp, including across restarts. Payloads, credentials, exception messages, causes and
suppressed exceptions remain excluded. There is no payload-capture mode.

Before enabling diagnostics, restrict access to the log destination and configure diagnostic log
retention to at most 24 hours. Remove the setting after the session and delete diagnostic exports
when the investigation ends. Retention is enforced by the log destination, not by this service.
Deployment verification must check the effective handlers, access-log exclusion and retention
through the public service. OAuth authorization-service logs require separate verification when
that service is integrated.

## API Feature enabling configuration
*Configuration for enabling Features, defined by [FeaturesConfig.java](src/main/java/io/stargate/sgv2/jsonapi/config/CommandLoggingConfig.java).*

| Property                        | Type      | Default | Description                                                                                                         |
|---------------------------------|-----------|---------|---------------------------------------------------------------------------------------------------------------------|
| `stargate.feature.flags.tables` | `boolean` | `true` (enabled by default)   | Setting it to `true` enables Tables functionality; `false` disables; leaving as `null` uses the default (enabled).|
