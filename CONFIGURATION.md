# Configuration Guide

> **IMPORTANT:** Please check the [Stargate Common Configuration](https://github.com/stargate/stargate/blob/main/apis/sgv2-quarkus-common/CONFIGURATION.md) for properties introduced by the `sgv2-quarkus-common` project.

## Quarkus Configuration

The complete list of Quarkus available properties can be found on [All configuration options](https://quarkus.io/guides/all-config) page.

Here are some Stargate-relevant property groups that are necessary for correct service setup:

* `quarkus.grpc.clients.bridge` - property group for defining the Bridge gRPC client (see [gRPC Client configuration](https://quarkus.io/guides/grpc-service-consumption#client-configuration) for all options)
* `quarkus.cache.caffeine.keyspace-cache` - property group  for defining the keyspace cache used by [SchemaManager](../sgv2-quarkus-common/src/main/java/io/stargate/sgv2/api/common/schema/SchemaManager.java) (see [Caffeine cache configuration](https://quarkus.io/guides/cache#caffeine-configuration-properties) for all options)


## Document limits configuration
*Configuration for document limits, defined by [DocumentLimitsConfig.java](src/main/java/io/stargate/sgv2/jsonapi/config/DocumentLimitsConfig.java).*

| Property                                                 | Type  | Default     | Description                                                                                                 |
|----------------------------------------------------------|-------|-------------|-------------------------------------------------------------------------------------------------------------|
| `stargate.jsonapi.document.limits.max-size`              | `int` | `1_000_000` | The maximum size of a single document in characters. Defaults to 1 million characters or approximately 1MB. |
| `stargate.jsonapi.document.limits.max-depth`             | `int` | `8`         | The maximum document depth (nesting).                                                                       |
| `stargate.jsonapi.document.limits.max-name-length`       | `int` | `48`        | The maximum length of property names in a document for an individual segment.                               |
| `stargate.jsonapi.document.limits.max-object-properties` | `int` | `64`        | The maximum number of properties any single object in a document can contain.                               |
| `stargate.jsonapi.document.limits.max-string-length`     | `int` | `16000`     | The maximum length of a single string value in a document.                                                  |
| `stargate.jsonapi.document.limits.max-array-length`      | `int` | `100`       | The maximum length of a single array in a document.                                                         |

## Light-weight transactions configuration
*Configuration for the light-weight transactions, defined by [LwtConfig.java](src/main/java/io/stargate/sgv2/jsonapi/config/LwtConfig.java).*

| Property                       | Type  | Default | Description                                                 |
|--------------------------------|-------|---------|-------------------------------------------------------------|
| `stargate.jsonapi.lwt.retries` | `int` | `3`     | The amount of client side retries in case of a LWT failure. |

## Document configuration
*Configuration for documents and their storage properties, defined by [DocumentConfig.java](src/main/java/io/stargate/sgv2/jsonapi/service/bridge/config/DocumentConfig.java).*

| Property                                        | Type     | Default      | Description                                                        |
|-------------------------------------------------|----------|--------------|--------------------------------------------------------------------|
| `stargate.document.page-size`                   | `int`    | `20`         | The maximum page size when reading documents.                      |