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

### Database readiness

`GET /v1/health/ready` is an authenticated database readiness endpoint used for both Astra and
Cassandra deployments. It uses the request's tenant, `Token` header, and `User-Agent` to obtain a
session through the normal session cache. The Data API does not store separate readiness
credentials.

The check is based on the driver's session metadata, which the driver populates from
`system.local` and `system.peers` when the session connects and keeps current through node state
events. The pod is ready when the session metadata reports at least one node in the `UP` state. No
query is issued against the database: acquiring the session is itself part of the check, because a
pod that cannot connect to the database fails session creation and reports `DOWN`. An `UP` response
therefore confirms that this pod holds a usable session with a live connection to the database. It
does not validate every tenant's credentials, quorum availability, write availability, or
cross-region availability.

Session creation itself carries a second, independent guard: `CqlSessionFactory` rejects any newly
built session whose driver metadata is missing the `system` keyspace, so a session that connected
but could not read the schema is never cached or handed to a request. That guard is always on and
applies to all session creation, not only readiness requests.

The two checks cover different moments, and the interaction matters for probe configuration. The
factory guard runs once, when a session is created; the readiness check runs on every probe against
whatever session is cached. Because a failed session creation surfaces as a failed session
acquisition, a probe token that authenticates but cannot read the schema tables makes this endpoint
report `DOWN` rather than an authorization error. Since readiness drives pod rotation, a probe token
whose schema-read permission is revoked will take every pod out of service. Grant the probe token
schema read access and alert on a fleet-wide `DOWN` transition, which indicates a credential problem
rather than a database outage.

The caller must send the full User-Agent configured by
`stargate.jsonapi.operations.sla-user-agent`. The comparison is case-insensitive. Requests with a
missing or different User-Agent are rejected before accessing the session cache, and the endpoint
fails closed when the SLA User-Agent is not configured. This ensures the probe session uses the
shorter SLA session TTL instead of being treated like normal client traffic. Do not reuse the probe
credentials for normal traffic, because using the same cached session with a non-SLA User-Agent can
extend its lifetime. Astra callers must use the probe database hostname so the tenant and region are
resolved from `Host`; Cassandra ignores the tenant portion of `Host`.

The check is fully asynchronous and has a five-second timeout. It returns HTTP 200 with
`{"status":"UP"}` when the session metadata reports an `UP` node and HTTP 503 with
`{"status":"DOWN"}` after a session failure, timeout, or missing SLA User-Agent configuration. It
returns the standard Data API error response with HTTP 401 when the `Token` header is missing or
authentication fails, and HTTP 403 when the request User-Agent does not match the configured SLA
User-Agent. Probe integrations must use the HTTP status as the readiness contract rather than
parsing the response body's `status` field alone.

Kubernetes or an SLA checker must call each pod directly for this endpoint to control per-pod
readiness. An external request sent through a load balancer does not establish which pod is ready.
Restrict the endpoint to trusted probe traffic with deployment controls such as a NetworkPolicy,
mTLS, or an ingress ACL and rate limit. The User-Agent check is an operational guard, not an
authentication boundary. Kubernetes `httpGet` headers cannot reference a Secret, so
delivery of the probe token is intentionally outside the Data API configuration. Prefer an
external checker or a Secret-mounted file read by an `exec` probe; do not put the token literally in
the probe command or shell trace. The unauthenticated Quarkus health endpoints under the
non-application path do not include this database check.


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
