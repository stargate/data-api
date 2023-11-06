# Configuration Guide

> **IMPORTANT:** Please check the [Stargate Common Configuration](https://github.com/stargate/stargate/blob/main/apis/sgv2-quarkus-common/CONFIGURATION.md) for properties introduced by the `sgv2-quarkus-common` project.

## Quarkus Configuration

The complete list of Quarkus available properties can be found on [All configuration options](https://quarkus.io/guides/all-config) page.

Here are some Stargate-relevant property groups that are necessary for correct service setup:

* `quarkus.grpc.clients.bridge` - property group for defining the Bridge gRPC client (see [gRPC Client configuration](https://quarkus.io/guides/grpc-service-consumption#client-configuration) for all options)
* `quarkus.cache.caffeine.keyspace-cache` - property group  for defining the keyspace cache used by [SchemaManager](../sgv2-quarkus-common/src/main/java/io/stargate/sgv2/api/common/schema/SchemaManager.java) (see [Caffeine cache configuration](https://quarkus.io/guides/cache#caffeine-configuration-properties) for all options)

## Database limits configuration
*Configuration for document limits, defined by [DatabaseLimitsConfig.java](src/main/java/io/stargate/sgv2/jsonapi/config/DatabaseLimitsConfig.java).*

| Property                                    | Type  | Default | Description                                                                          |
|---------------------------------------------|-------|---------|--------------------------------------------------------------------------------------|
| `stargate.database.limits.max-collections`  | `int` | `5`     | The maximum number of Collections allowed to be created per Database: defaults to 5. |

## Document limits configuration
*Configuration for document limits, defined by [DocumentLimitsConfig.java](src/main/java/io/stargate/sgv2/jsonapi/config/DocumentLimitsConfig.java).*

| Property                                                    | Type  | Default     | Description                                                                                                 |
|-------------------------------------------------------------|-------|-------------|-------------------------------------------------------------------------------------------------------------|
| `stargate.jsonapi.document.limits.max-size`                 | `int` | `1_000_000` | The maximum size of a single document in characters. Defaults to 1 million characters or approximately 1MB. |
| `stargate.jsonapi.document.limits.max-depth`                | `int` | `8`         | The maximum document depth (nesting).                                                                       |
| `stargate.jsonapi.document.limits.max-property-name-length` | `int` | `48`        | The maximum length of property names in a document for an individual segment.                               |
| `stargate.jsonapi.document.limits.max-object-properties`    | `int` | `64`        | The maximum number of properties any single object in a document can contain.                               |
| `stargate.jsonapi.document.limits.max-string-length`        | `int` | `16_000`    | The maximum length of a single string value in a document.                                                  |
| `stargate.jsonapi.document.limits.max-array-length`         | `int` | `100`       | The maximum length of a single array in a document.                                                         |

## Operations configuration
*Configuration for the operation execution, defined by [OperationsConfig.java](src/main/java/io/stargate/sgv2/jsonapi/config/OperationsConfig.java).*

| Property                                                 | Type  | Default  | Description                                                                                                                                                                                        |
|----------------------------------------------------------|-------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `stargate.jsonapi.operations.default-page-size`          | `int` | `20`     | The default Cassandra page size used for read queries.                                                                                                                                             |
| `stargate.jsonapi.operations.default-sort-page-size`     | `int` | `100`    | The default Cassandra page size used for read queries that are used for sorting purposes.                                                                                                          |
| `stargate.jsonapi.operations.max-document-sort-count`    | `int` | `10_000` | The maximum amount of documents that could be sorted using the in-memory sorting. The request will fail in case in-memory sorting would break the limit.                                           |
| `stargate.jsonapi.operations.max-document-insert-count`  | `int` | `20`     | The maximum amount of documents that can be inserted in a single operation. The request will fail fast without inserts if the limit is broken.                                                     |
| `stargate.jsonapi.operations.max-document-update-count`  | `int` | `20`     | The maximum amount of documents that can be updated in a single operation. In case there are more documents that could be updated, the operation will set the `moreData` response status to `true`. |
| `stargate.jsonapi.operations.max-document-delete-count`  | `int` | `20`     | The maximum amount of documents that can be deleted in a single operation. In case there are more documents that could be deleted, the operation will set the `moreData` response status to `true`. |
| `stargate.jsonapi.operations.max-in-operator-value-size` | `int` | `100`    | The maximum number of _id values that can be passed for `$in` operator.   |
| `stargate.jsonapi.operations.lwt.retries`                | `int` | `3`      | The amount of client side retries in case of a LWT failure.                                                                                                                                        |

## Jsonapi metering configuration
*Configuration for jsonapi metering, defined by [JsonApiMetricsConfig.java](io/stargate/sgv2/jsonapi/api/v1/metrics/JsonApiMetricsConfig.java).*

| Property                              | Type     | Default       | Description                                                  |
|---------------------------------------|----------|---------------|--------------------------------------------------------------|
| `stargate.jsonapi.metric.error-class` | `string` | `error.class` | Metrics tag that provides information about the error class. |
| `stargate.jsonapi.metric.error-code`  | `string` | `error.code`  | Metrics tag that provides information about the error code.  |
| `stargate.jsonapi.metric.command`     | `string` | `command`     | Metrics tag that provides information about the command.     |
| `stargate.jsonapi.metric.metrics.name`| `string` | `jsonapi`     | Metrics name prefix.                                         |
