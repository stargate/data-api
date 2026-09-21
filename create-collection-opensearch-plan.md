# Create collection OpenSearch plan

## Overview

Extend `createCollection` with an optional `options.openSearch` configuration that creates an HCD `OpenSearchIndex` for a super-shredded collection. The implementation accepts the revised `mappings` contract from the authoritative HCD-to-Data-API specification, validates enabled and disabled configurations consistently with lexical options, persists enabled settings in a V3 collection comment, and emits the corresponding custom-index DDL.

Scope is limited to `createCollection` support and its schema/introspection plumbing. It excludes `$search`, table-level `createOpenSearchIndex`, aggregations, and count support, which are separate commands in the specification.

## Sub-tasks

### 1. Remove obsolete local specification

- **Status:** [ ] pending
- **Intent:** Remove the stale local OpenSearch proposal so it cannot be mistaken for the authoritative, externally maintained requirements document.
- **Expected outcomes:** The repository no longer contains `specs-h2o.md`; planning and implementation reference `../hcd-to-data-api-specs/specifications/2-specs-hcd-to-opensearch-data-api.md`.
- **Todo list:**
  1. Delete `specs-h2o.md`.
  2. Verify no project documentation links or test fixtures still reference it.
- **Relevant context:** `specs-h2o.md`; `../hcd-to-data-api-specs/specifications/2-specs-hcd-to-opensearch-data-api.md`.

### 2. Add and validate the createCollection API option

- **Status:** [ ] pending
- **Intent:** Expose the revised `options.openSearch` payload and enforce a coherent enabled or disabled configuration before any schema changes are attempted.
- **Expected outcomes:** `createCollection` accepts `enabled`, `indexName`, `numShards`, `numReplicas`, and `mappings`; invalid configurations produce documented schema errors.
- **Todo list:**
  1. Add an `OpenSearchDesc` API record and nullable `openSearch` member to `CreateCollectionCommand.Options`.
  2. Require `enabled` whenever the block is supplied.
  3. Mirror lexical disabled-option semantics: with `enabled: false`, permit omitted, JSON-null, or empty-object `mappings`; reject a non-empty mapping and any configured index name, shard count, or replica count.
  4. For `enabled: true`, require a non-empty object `mappings`; reject `_id` as a top-level mapping key; validate numeric fields as positive when supplied.
  5. When enabled, reject a simultaneous `indexing.deny`; allow `indexing.allow` independently of OpenSearch mappings.
  6. Add the required schema error codes and their message templates, retaining `OPEN_SEARCH_MISSING_FIELD_MAPPINGS` as specified despite the API field being named `mappings`.
  7. Add API-model and resolver unit tests for valid payloads, defaults, required enabled flag, disabled-state validation, mappings validation, and deny-list incompatibility.
- **Relevant context:** `src/main/java/io/stargate/sgv2/jsonapi/api/model/command/impl/CreateCollectionCommand.java`; `src/main/java/io/stargate/sgv2/jsonapi/service/resolver/CreateCollectionCommandResolver.java`; `src/main/java/io/stargate/sgv2/jsonapi/exception/SchemaException.java`; `src/main/java/io/stargate/sgv2/jsonapi/service/schema/collections/CollectionLexicalDef.java`; `src/test/java/io/stargate/sgv2/jsonapi/api/v1/CreateCollectionWithLexicalIntegrationTest.java`.

### 3. Represent enabled OpenSearch settings in collection schema V3

- **Status:** [ ] pending
- **Intent:** Persist resolved enabled OpenSearch configuration in collection metadata and make it available for collection introspection and later OpenSearch read-command work, without altering legacy collections.
- **Expected outcomes:** Enabled configurations round-trip through collection comments and `findCollections` explain output; disabled and legacy V1/V2 collections omit `openSearch` from their exposed options.
- **Todo list:**
  1. Introduce `CollectionOpenSearchDef` with schema defaults, API conversion, and the resolved settings required by the feature: enabled state, index name, shard count, replica count, mappings, and generated SAI index name as required by later read routing.
  2. Add a `CollectionOpenSearchDefSchemaFactory` and register it with `SchemaRegistry`; follow the existing feature-gating pattern after confirming the required HCD feature-flag design.
  3. Add `openSearchDef` to `CollectionSchemaObject`, including constructors, accessors, diagnostic recording, equality-driven idempotence, and conversion back to `CreateCollectionCommand` options.
  4. Add `V_3` as the current collection schema version, add the `openSearch` table-comment constant, and serialize enabled resolved settings in generated comments. Omit the key for disabled OpenSearch.
  5. Implement `CollectionSettingsV3Reader`; preserve V1/V2 behavior by returning the pre-release disabled default when no OpenSearch key exists.
  6. Reject corrupt V3 persisted settings that claim enabled OpenSearch but lack the resolved index name, using the specified corrupt-schema error.
  7. Add schema unit and integration tests for V3 comment round trips, legacy V1/V2 fallback, disabled omission, corrupt enabled settings, explain serialization, and repeated-create equality checks.
- **Relevant context:** `src/main/java/io/stargate/sgv2/jsonapi/service/schema/collections/CollectionSchemaObject.java`; `src/main/java/io/stargate/sgv2/jsonapi/service/schema/CollectionSchemaVersion.java`; `src/main/java/io/stargate/sgv2/jsonapi/service/schema/collections/CollectionSettingsV1Reader.java`; `src/main/java/io/stargate/sgv2/jsonapi/service/schema/collections/CollectionSettingsV2Reader.java`; `src/main/java/io/stargate/sgv2/jsonapi/service/schema/SchemaRegistry.java`; `src/main/java/io/stargate/sgv2/jsonapi/config/constants/TableCommentConstants.java`.

### 4. Create the HCD OpenSearch custom index with collection creation

- **Status:** [ ] pending
- **Intent:** Create the single HCD replication index immediately after the backing collection table, using mappings that are compatible with the collection `doc_json` storage model.
- **Expected outcomes:** An enabled OpenSearch collection issues `CREATE CUSTOM INDEX ... USING 'OpenSearchIndex'` with correctly derived names and options. Collections without enabled OpenSearch retain their existing DDL path.
- **Todo list:**
  1. Pass the resolved OpenSearch schema definition from the resolver through `CreateCollectionOperation`.
  2. Resolve the default OS index name as `hcd_<keyspace>_<collection>` during enabled configuration processing; derive the auto-generated Cassandra SAI index identifier using the same name pattern.
  3. Add the OpenSearch custom-index statement to the post-table index sequence only when enabled, preserving current creation ordering, idempotent replay behavior, rollback handling, and index-limit accounting.
  4. Generate the mandatory HCD options: resolved `indexName`, `createIndexIfNotExists=true`, `unpackJsonFields=doc_json`, `applyDefaultSchema=false`, `applyCustomSchema=true`, serialized `customMappingsJson`, and `propertiesFromJsonFields`.
  5. Build `customMappingsJson` as a `properties` object that prepends `_id` as `keyword` to the user mappings; derive `propertiesFromJsonFields` from only the user mapping keys so `_id` is excluded.
  6. Include `numShards` and `numReplicas` only when provided; do not emit the removed skip-build option.
  7. Add operation tests that inspect the custom-index CQL/options, test defaults and explicit names/counts, and verify existing index-count and rollback behavior remains correct.
- **Relevant context:** `src/main/java/io/stargate/sgv2/jsonapi/service/operation/collections/CreateCollectionOperation.java`; `src/main/java/io/stargate/sgv2/jsonapi/service/cqldriver/override/ExtendedCreateIndex.java`; `src/main/java/io/stargate/sgv2/jsonapi/service/schema/tables/CQLSAIIndex.java`; `src/test/java/io/stargate/sgv2/jsonapi/service/operation/collections/CreateCollectionOperationTest.java`.

### 5. Verify end-to-end behavior and protect compatibility

- **Status:** [ ] pending
- **Intent:** Confirm that the new command contract works through the public API without regressing existing collection creation modes.
- **Expected outcomes:** Integration coverage demonstrates enabled creation and introspection, disabled behavior, error responses, idempotent recreation, and unchanged non-OpenSearch collection behavior.
- **Todo list:**
  1. Add integration tests for minimal enabled creation, explicit index name, shard and replica options, mappings containing nested definitions, and `indexing.allow` combined with OpenSearch.
  2. Add negative integration tests for missing `enabled`, missing or empty mappings, `_id` mappings, deny-list incompatibility, and populated disabled configurations.
  3. Test `enabled: false` with permitted absent/null/empty mappings, asserting no custom index and no explain response block.
  4. Test identical repeated creates succeed and changed OpenSearch settings are rejected as different collection settings.
  5. Run focused unit and integration suites, followed by the project’s relevant format, compilation, and test validation commands.
- **Relevant context:** `src/test/java/io/stargate/sgv2/jsonapi/api/v1/CreateCollectionIntegrationTest.java`; `src/test/java/io/stargate/sgv2/jsonapi/api/v1/FindCollectionsIntegrationTest.java`; `src/test/java/io/stargate/sgv2/jsonapi/api/v1/CreateCollectionWithLexicalIntegrationTest.java`.
