# Configuration Guide

> **IMPORTANT:** Please check the [Stargate Common Configuration](https://github.com/stargate/stargate/blob/main/apis/sgv2-quarkus-common/CONFIGURATION.md) for properties introduced by the `sgv2-quarkus-common` project.

## Quarkus Configuration

The complete list of Quarkus available properties can be found on [All configuration options](https://quarkus.io/guides/all-config) page.

Here are some Stargate-relevant property groups that are necessary for correct service setup:

* `quarkus.grpc.clients.bridge` - property group for defining the Bridge gRPC client (see [gRPC Client configuration](https://quarkus.io/guides/grpc-service-consumption#client-configuration) for all options)
* `quarkus.cache.caffeine.keyspace-cache` - property group  for defining the keyspace cache used by [SchemaManager](../sgv2-quarkus-common/src/main/java/io/stargate/sgv2/api/common/schema/SchemaManager.java) (see [Caffeine cache configuration](https://quarkus.io/guides/cache#caffeine-configuration-properties) for all options)

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

| Property                                                                | Type  | Default  | Description                                                                                                                                                                                         |
|-------------------------------------------------------------------------|-------|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `stargate.jsonapi.operations.default-page-size`                         | `int` | `20`     | The default Cassandra page size used for read queries.                                                                                                                                              |
| `stargate.jsonapi.operations.default-sort-page-size`                    | `int` | `100`    | The default Cassandra page size used for read queries that are used for sorting purposes.                                                                                                           |
| `stargate.jsonapi.operations.max-document-sort-count`                   | `int` | `10_000` | The maximum amount of documents that could be sorted using the in-memory sorting. The request will fail in case in-memory sorting would break the limit.                                            |
| `stargate.jsonapi.operations.max-document-insert-count`                 | `int` | `20`     | The maximum amount of documents that can be inserted in a single operation. The request will fail fast without inserts if the limit is broken.                                                      |
| `stargate.jsonapi.operations.max-document-update-count`                 | `int` | `20`     | The maximum amount of documents that can be updated in a single operation. In case there are more documents that could be updated, the operation will set the `moreData` response status to `true`. |
| `stargate.jsonapi.operations.max-document-delete-count`                 | `int` | `20`     | The maximum amount of documents that can be deleted in a single operation. In case there are more documents that could be deleted, the operation will set the `moreData` response status to `true`. |
| `stargate.jsonapi.operations.max-filter-object-properties`              | `int` | `64`     | The maximum number of properties a single filter clause can contain.                                                                                                                                |
| `stargate.jsonapi.operations.max-in-operator-value-size`                | `int` | `100`    | The maximum number of _id values that can be passed for `$in` operator.                                                                                                                             |
| `stargate.jsonapi.operations.lwt.retries`                               | `int` | `3`      | The amount of client side retries in case of a LWT failure.                                                                                                                                         |
| `stargate.jsonapi.operations.database-config.session-cache-ttl-seconds` | `int` | `300`    | The amount of seconds that the cql session will be kept in memory after last access.                                                                                                                |
| `stargate.jsonapi.operations.database-config.session-cache-max-size`    | `int` | `50`     | The maximum number of cql sessions that will be kept in memory.                                                                                                                                     |
| `stargate.jsonapi.operations.default-count-page-size`                   | `int` | `100`    | The default Cassandra page size used for reading keys for count command.                                                                                                                            |
| `stargate.jsonapi.operations.max-count-limit`                           | `int` | `1000`   | The default maximum number of rows to read for count operation.                                                                                                                                     |
| `stargate.jsonapi.operations.database-config.ddl-retry-delay-millis`    | `int` | `1000`   | Delay time in seconds for DDL timeout.                                                                                                                                                              |
| `stargate.jsonapi.operations.database-config.ddl-delay-millis`               | `int` | `2000`   | Delay between create table and create index to get the schema sync.                                                                                                                                 |


## Jsonapi metering configuration
*Configuration for jsonapi metering, defined by [JsonApiMetricsConfig.java](io/stargate/sgv2/jsonapi/api/v1/metrics/JsonApiMetricsConfig.java).*

| Property                              | Type     | Default       | Description                                                  |
|---------------------------------------|----------|---------------|--------------------------------------------------------------|
| `stargate.jsonapi.metric.error-class` | `string` | `error.class` | Metrics tag that provides information about the error class. |
| `stargate.jsonapi.metric.error-code`  | `string` | `error.code`  | Metrics tag that provides information about the error code.  |
| `stargate.jsonapi.metric.command`     | `string` | `command`     | Metrics tag that provides information about the command.     |
| `stargate.jsonapi.metric.metrics.name`| `string` | `jsonapi`     | Metrics name prefix.                                         |


## Command level logging configuration
*Configuration for command level logging, defined by [CommandLoggingConfig.java](src/main/java/io/stargate/sgv2/jsonapi/config/CommandLoggingConfig.java).*

| Property                                            | Type      | Default | Description                                                                                                                                                        |
|-----------------------------------------------------|-----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `stargate.jsonapi.logging.enabled`                  | `boolean` | `false` | Setting it to `true` enables command level logging.                                                                                                                |
| `stargate.jsonapi.logging.only-results-with-errors` | `boolean` | `true`  | Setting it to `true` prints the command level info only for the commands where the command result has errors.                                                      |
| `stargate.jsonapi.logging.enabled-tenants`          | `string`  | `ALL`   | Comma separated list of tenants for which command level logging should be enabled. Default is a special keyword called `ALL` which prints this log for all tenants |
