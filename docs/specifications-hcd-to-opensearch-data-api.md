# Data API — HCD OpenSearch Integration Plan

## Table of Contents

- [1. Top-Level Overview](#1-top-level-overview)
- [2. Architecture Overview](#2-architecture-overview)
- [3. Context — How HCD-to-OpenSearch Works](#3-context--how-hcd-to-opensearch-works)
  - [3.1 Constraints specific to Data API collections (super-shredded schema)](#31-constraints-specific-to-data-api-collections-super-shredded-schema)
    - [3.1.1 Handling EJSON Extension Types in Collections (`$date`, `$binary`, `$uuid`, `$objectId`)](#311-handling-ejson-extension-types-in-collections-date-binary-uuid-objectid)
    - [3.1.2 Special Characters in Property Names (Dots, Ampersands, Dollar Signs)](#312-special-characters-in-property-names-dots-ampersands-dollar-signs)
- [4. Impacted Commands — Collections](#4-impacted-commands--collections)
  - [4.1 `createCollection` — new `openSearch` option block](#41-createcollection--new-opensearch-option-block)
    - [4.1.1 Goal](#411-goal)
    - [4.1.2 How it works](#412-how-it-works)
    - [4.1.3 OpenSearch fields in the Data API Payload](#413-opensearch-fields-in-the-data-api-payload)
    - [4.1.4 Sample payloads](#414-sample-payloads)
    - [4.1.5 Implementation Tips (server)](#415-implementation-tips-server)
    - [4.1.6 Implementation Tips (clients)](#416-implementation-tips-clients)
  - [4.2 `findCollections` — return `openSearch` block in `explain` response](#42-findcollections--return-opensearch-block-in-explain-response)
    - [4.2.1 Goal](#421-goal)
    - [4.2.2 How it works](#422-how-it-works)
    - [4.2.3 OpenSearch fields in the Data API Payload](#423-opensearch-fields-in-the-data-api-payload)
    - [4.2.4 Sample payloads](#424-sample-payloads)
    - [4.2.5 Implementation Tips (server)](#425-implementation-tips-server)
    - [4.2.6 Implementation Tips (clients)](#426-implementation-tips-clients)
  - [4.3 `alterCollection` — add or drop the OpenSearch index on an existing collection](#43-altercollection--add-or-drop-the-opensearch-index-on-an-existing-collection)
    - [4.3.1 Goal](#431-goal)
    - [4.3.2 How it works](#432-how-it-works)
    - [4.3.3 OpenSearch fields in the Data API Payload](#433-opensearch-fields-in-the-data-api-payload)
    - [4.3.4 Sample payloads](#434-sample-payloads)
    - [4.3.5 Idempotency and `createCollection` convergence](#435-idempotency-and-createcollection-convergence)
    - [4.3.6 Implementation Tips (server)](#436-implementation-tips-server)
    - [4.3.7 Implementation Tips (clients)](#437-implementation-tips-clients)
  - [4.4 `find` on Collections — new `$openSearch` filter operator](#44-find-on-collections--new-search-filter-operator)
    - [4.4.1 Goal](#441-goal)
    - [4.4.2 How it works](#442-how-it-works)
    - [4.4.3 OpenSearch fields in the Data API Payload](#443-opensearch-fields-in-the-data-api-payload)
    - [4.4.4 Sample payloads](#444-sample-payloads)
    - [4.4.4b Paging strategies — sample payloads](#444b-paging-strategies--sample-payloads)
    - [4.4.5 Implementation Tips (server)](#445-implementation-tips-server)
    - [4.4.6 Implementation Tips (clients)](#446-implementation-tips-clients)
  - [4.5 `searchAggregate` on Collections — aggregations and faceted search](#45-searchaggregate-on-collections--aggregations-and-faceted-search)
    - [4.5.1 Goal](#451-goal)
    - [4.5.2 How it works](#452-how-it-works)
    - [4.5.3 OpenSearch fields in the Data API Payload](#453-opensearch-fields-in-the-data-api-payload)
    - [4.5.4 Sample payloads](#454-sample-payloads)
    - [4.5.5 Implementation Tips (server)](#455-implementation-tips-server)
    - [4.5.6 Implementation Tips (clients)](#456-implementation-tips-clients)
  - [4.6 `countDocuments` with `$openSearch` filter](#46-countdocuments-with-search-filter)
    - [4.6.1 Goal](#461-goal)
    - [4.6.2 How it works](#462-how-it-works)
    - [4.6.3 OpenSearch fields in the Data API Payload](#463-opensearch-fields-in-the-data-api-payload)
    - [4.6.4 Sample payloads](#464-sample-payloads)
    - [4.6.5 Implementation Tips (server)](#465-implementation-tips-server)
    - [4.6.6 Implementation Tips (clients)](#466-implementation-tips-clients)
  - [4.7 `deleteCollection` — also drops the OpenSearch index](#47-deletecollection--also-drops-the-opensearch-index)
    - [4.7.1 Goal](#471-goal)
    - [4.7.2 How it works](#472-how-it-works)
    - [4.7.3 Implementation Tips (server)](#473-implementation-tips-server)
    - [4.7.4 Implementation Tips (clients)](#474-implementation-tips-clients)
- [5. Impacted Commands — Tables](#5-impacted-commands--tables)
  - [5.1 `createOpenSearchIndex` — new DDL command for Tables](#51-createopensearchindex--new-ddl-command-for-tables)
    - [5.1.1 Goal](#511-goal)
    - [5.1.2 How it works](#512-how-it-works)
    - [5.1.3 OpenSearch fields in the Data API Payload](#513-opensearch-fields-in-the-data-api-payload)
    - [5.1.4 Sample payloads](#514-sample-payloads)
    - [5.1.5 Implementation Tips (server)](#515-implementation-tips-server)
    - [5.1.6 Implementation Tips (clients)](#516-implementation-tips-clients)
  - [5.2 `find` on Tables — new `$openSearch` filter operator](#52-find-on-tables--new-search-filter-operator)
    - [5.2.1 Goal](#521-goal)
    - [5.2.2 How it works](#522-how-it-works)
    - [5.2.3 OpenSearch fields in the Data API Payload](#523-opensearch-fields-in-the-data-api-payload)
    - [5.2.4 Sample payloads](#524-sample-payloads)
    - [5.2.5 Implementation Tips (server)](#525-implementation-tips-server)
    - [5.2.6 Implementation Tips (clients)](#526-implementation-tips-clients)
  - [5.3 `listIndexes` on Tables — includes OpenSearch indexes](#53-listindexes-on-tables--includes-opensearch-indexes)
    - [5.3.1 Goal](#531-goal)
    - [5.3.2 Response shape](#532-response-shape)
    - [5.3.3 Sample payloads](#533-sample-payloads)
    - [5.3.4 Implementation Tips (server)](#534-implementation-tips-server)
    - [5.3.5 Implementation Tips (clients)](#535-implementation-tips-clients)
  - [5.4 `dropIndex` on Tables — drops the OpenSearch custom index](#54-dropindex-on-tables--drops-the-opensearch-custom-index)
    - [5.4.1 Goal](#541-goal)
    - [5.4.2 How it works](#542-how-it-works)
    - [5.4.3 Implementation Tips (server)](#543-implementation-tips-server)
    - [5.4.4 Implementation Tips (clients)](#544-implementation-tips-clients)
  - [5.5 `searchAggregate` on Tables — aggregations and faceted search](#55-searchaggregate-on-tables--aggregations-and-faceted-search)
    - [5.5.1 Goal](#551-goal)
    - [5.5.2 How it works](#552-how-it-works)
    - [5.5.3 OpenSearch fields in the Data API Payload](#553-opensearch-fields-in-the-data-api-payload)
    - [5.5.4 Sample payloads](#554-sample-payloads)
    - [5.5.5 Implementation Tips (server)](#555-implementation-tips-server)
    - [5.5.6 Implementation Tips (clients)](#556-implementation-tips-clients)
- [6. Cross-Cutting Concerns](#6-cross-cutting-concerns)
  - [6.1 Class hierarchy](#61-class-hierarchy)
  - [6.2 Schema version evolution](#62-schema-version-evolution)
  - [6.3 Error codes](#63-error-codes)
  - [6.4 `CollectionSchemaVersion`](#64-collectionschemaversion)
  - [6.5 `isSupported()` check in `IndexFactoryFromCql`](#65-issupported-check-in-indexfactoryfromcql)
  - [6.6 HCD feature flag](#66-hcd-feature-flag)
  - [6.7 Testing](#67-testing)
  - [6.8 Write consistency and read-after-write guarantees](#68-write-consistency-and-read-after-write-guarantees)
- [7. Appendix — `OpenSearchQueryBuilder` API Reference](#7-appendix--opensearchquerybuilder-api-reference)
  - [7.1 Query builders](#71-query-builders)
  - [7.2 Aggregation builders](#72-aggregation-builders)
  - [7.3 Sort builders](#73-sort-builders)
  - [7.4 Cross-language usage summary](#74-cross-language-usage-summary)

---

## 1. Top-Level Overview

> ### Terminology — two names, two systems
> Every OpenSearch-backed table carries **two completely separate index names** that are used in different places and must never be confused. This distinction appears throughout the entire document; keeping it clear is essential to understanding the API design.
>
> | Term | Definition | Where it appears |
> |---|---|---|
> | **SAI index name** | The Cassandra custom index identifier — the name after `CREATE CUSTOM INDEX … IF NOT EXISTS`. Used inside CQL `expr(<sai-index-name>, …)` predicates. For **collections** it is always auto-generated as `hcd_<keyspace>_<collectionName>` and is **not user-configurable**. For **tables** it is the `name` field supplied to `createOpenSearchIndex` and **is user-configurable**. | CQL statements, `expr(...)` calls, all server-side routing |
> | **OS index name** | The OpenSearch index name — the value of the `'indexName'` key inside the CQL `WITH OPTIONS` block. This is the index that OpenSearch actually stores and searches. For **collections** it is configurable via `openSearch.indexName`; for **tables** it is configurable via `definition.options.openSearchIndexName`. When omitted it defaults to `hcd_<keyspace>_<tableName>`. | OpenSearch REST API, `WITH OPTIONS 'indexName'` |
>
> A single CQL statement carries both: `CREATE CUSTOM INDEX IF NOT EXISTS <sai-index-name> … WITH OPTIONS = {'indexName': '<os-index-name>', …}`. The two names may be identical (the default) or different (when the user sets `indexName`).

**Goal:** Extend the Data API to expose the new HCD OpenSearch replication capability, which mirrors Cassandra writes to an OpenSearch index in real time. This surfaces ten impacted API surfaces across collections and tables:

**Collections (§4):**
1. **`createCollection`** — a new `openSearch` option block configures the underlying `CREATE CUSTOM INDEX … USING 'OpenSearchIndex'` on the super-shredded collection table.
2. **`findCollections`** — with `explain: true`, returns the resolved `openSearch` block (including the auto-generated OS index name) for OpenSearch-enabled collections. The block is updated whenever the OpenSearch configuration changes via `alterCollection`.
3. **`alterCollection`** — new operations `addOpenSearch` / `dropOpenSearch` allow adding or removing an OpenSearch replication index on an existing collection, independent of collection creation.
4. **Collection `find`** — a new `$openSearch` operator in the filter clause that routes the query to OpenSearch via CQL `expr(<saiIndexName>, '<OpenSearch JSON>')`.
5. **`searchAggregate`** — a new command that passes OpenSearch aggregation DSL (`aggs`) through `expr(...)` and returns structured facet / metric results.
6. **`countDocuments` with `$openSearch`** — the existing `countDocuments` command is extended to accept a `$openSearch` filter and return the OpenSearch `numFound` count.
7. **`deleteCollection`** — must also drop the OpenSearch custom index before dropping the table; same API signature, new implementation step.

**Tables (§5):**
8. **`createOpenSearchIndex`** — a dedicated DDL command (parallel to `createTextIndex` / `createVectorIndex`) that creates an OpenSearch custom index on a user-defined table.
9. **Table `find`** — the same `$openSearch` operator is made available on table reads, backed by `expr(...)`.
10. **`dropIndex`** — must handle `indexType: openSearch`; dropping the Cassandra custom index causes HCD to also delete the OS index from the OpenSearch cluster.

> **`$openSearch` is exclusive — it cannot be combined with regular field filters.** The `$openSearch` key must always be the only key in the `filter` object. This is a fundamental CQL constraint: HCD's `expr(...)` predicate takes full ownership of the read path and cannot be ANDed with ordinary column predicates. See §3 for the full explanation. The same exclusivity rule applies everywhere `$openSearch` is accepted: `find`, `findOne`, `findOneAndUpdate`, and all other commands that accept a filter.

**Scope constraints (what this plan does NOT cover):**
- Any changes to the HCD / OpenSearch replication component itself.
- Dynamic schema / custom mapping management — the initial API surfaces sensible defaults; advanced mapping is an operator-level concern.
- Backfill / rebuild operations are not part of the Data API surface.
- OpenSearch highlighting, autocomplete/suggest, and scroll cursor — not feasible because `expr(...)` → CQL row conversion drops all OpenSearch hit metadata (see discussion docs).

---

## 2. Architecture Overview

> **Note:** The diagrams below are intentionally simplified — they show the core write and read paths only. DDL commands (`createCollection`, `createOpenSearchIndex`), `countDocuments`, `searchAggregate`, and `listCollections` follow the same structural patterns and are covered in their respective sections (§4.1–§4.7) rather than here.

> **Diagram 1a — Write path.** Every document write is automatically mirrored to OpenSearch by HCD with no Data API involvement beyond the initial DDL.

```mermaid
flowchart LR
    Client["Client\ninsertOne / updateOne\n/ deleteOne"]
    DataAPI["Data API\nnormal CQL write\n(no change)"]
    Cassandra["Cassandra\nCQL Layer"]
    OMI["HCD\nOpenSearchMutatorInterceptor\nwrite-path hook"]
    BP["HCD\nBulkProcessor\nbatches up to 1 000 docs\nflushes every 1 s"]
    OS["OpenSearch\nBulk Index API"]

    Client -->|"HTTP POST"| DataAPI
    DataAPI -->|"INSERT / UPDATE / DELETE\n(consistency upgraded to QUORUM)"| Cassandra
    Cassandra -->|"intercept mutation"| OMI
    OMI -->|"buffer"| BP
    BP -->|"POST /_bulk"| OS
```

> **Diagram 1b — Read path.** `$openSearch`, `searchAggregate`, and `countDocuments` with `$openSearch` all follow this path. The Data API serialises the OpenSearch DSL into a CQL `expr(saiIndexName, dsl)` predicate; HCD intercepts it, queries OpenSearch for matching PKs, and returns the corresponding Cassandra rows.

```mermaid
flowchart LR
    Client["Client\nfind / findOne\nsearchAggregate\ncountDocuments"]
    DataAPI["Data API\nbuilds expr(saiIndexName, dsl)\nfrom filter.$openSearch or aggregations"]
    Cassandra["Cassandra\nCQL Layer"]
    OQH["HCD\nOpenSearchQueryHandler\nread-path hook"]
    OS["OpenSearch\n/_search or /_count"]

    Client -->|"HTTP POST"| DataAPI
    DataAPI -->|"SELECT … WHERE expr(saiIndexName, dsl)"| Cassandra
    Cassandra -->|"intercept expr() predicate"| OQH
    OQH -->|"search DSL"| OS
    OS -->|"matching doc PKs"| OQH
    OQH -->|"Cassandra rows by PK"| Cassandra
    Cassandra -->|"rows"| DataAPI
    DataAPI -->|"response"| Client
```

---

## 3. Context — How HCD-to-OpenSearch Works

The HCD OpenSearch component hooks into the **Cassandra write path** (`OpenSearchMutatorInterceptor`) and replicates every mutation in real time to a connected OpenSearch cluster. The feature is activated per-table by creating a single special Cassandra custom index.

> **Two distinct names.** Every OpenSearch-backed Cassandra table carries **two separate index names** that serve completely different purposes and must never be confused:
> - **SAI index name** — the Cassandra custom index identifier (the name after `CREATE CUSTOM INDEX … IF NOT EXISTS`). This name is used inside CQL `expr()` predicates to route a query to the HCD OpenSearch handler. For collections the Data API auto-generates it as `hcd_<keyspace>_<collectionName>`; for tables the user supplies it via the `name` field of `createOpenSearchIndex`.
> - **OS index name** — the OpenSearch index name, passed as the `'indexName'` key inside the CQL `WITH OPTIONS` block. This is the index that OpenSearch actually stores and searches. It can be set freely and independently of the SAI index name; when omitted it also defaults to `hcd_<keyspace>_<tableName>`.

```cql
CREATE CUSTOM INDEX IF NOT EXISTS <sai-index-name>
ON <keyspace>.<table> (<columns>)
USING 'OpenSearchIndex'
WITH OPTIONS = {
    'indexName':              '<os-index-name>',
    'createIndexIfNotExists': 'true',
    'unpackJsonFields':       'doc_json',
    'propertiesFromJsonFields': 'field1,field2'
};
```

Only **one** OpenSearch custom index (and therefore one OS index) is allowed per Cassandra table. Querying goes through a CQL custom expression that references the **SAI index name**:

```cql
SELECT * FROM <table> WHERE expr(<sai-index-name>, '<OpenSearch-query-JSON>')
```

> **`expr(...)` must be the sole predicate — no AND with regular column filters.** HCD's `OpenSearchQueryHandler` intercepts the `expr(...)` predicate and takes over the entire row-fetch path: it queries OpenSearch for matching PKs and returns the corresponding Cassandra rows directly. Because the handler owns the full result set, `expr(...)` cannot be mixed with regular CQL predicates (`column = value`, `IN`, range clauses, etc.) in the same `WHERE` clause. Any attempt to do so is rejected at the CQL layer. This is a CQL/HCD constraint, not a Data API design choice, and it is why the Data API `$openSearch` filter must always be the **only** key in the filter object — there is no way to push additional predicates down to the same CQL query. Post-fetch filtering (in-memory) is technically possible but is not supported in this version.

### 3.1 Constraints specific to Data API collections (super-shredded schema)

The collection shredding model (`SuperShreddingMetadata`) stores the raw JSON in `doc_json TEXT` alongside auxiliary indexing columns (`exist_keys`, `query_text_values`, etc.). To make document fields searchable in OpenSearch:

The following four `WITH OPTIONS` keys are **always required** for collections — confirmed by integration testing. There is no optional or auto-inferred path:

1. **`unpackJsonFields: 'doc_json'`** — HCD must parse the JSON blob and merge properties at the OS document root.
2. **`propertiesFromJsonFields: '<fields>'`** — acts as the allow-list of fields to extract. Must **never** include `_id` — OpenSearch permanently rejects `_id` as a body field (`mapper_parsing_exception`), no mapping can override this.
3. **`applyCustomSchema: 'true'` + `customMappingsJson: '<json>'`** — required because the Cassandra column is `doc_json TEXT` with no typed schema; `applyDefaultSchema: 'true'` has no schema to work from. The `customMappingsJson` must cover the same fields listed in `propertiesFromJsonFields` and must **not** include an `_id` entry (that field is injected by the Data API automatically as `{"type":"keyword"}`).
4. **`applyDefaultSchema: 'false'`** — must accompany `applyCustomSchema: 'true'`.

- The feature is configured at **collection creation time** and is **immutable** afterward (same as `lexical` and `rerank`), because OpenSearch mappings cannot be changed after index creation.
- Consistency level for writes to OS-indexed tables is automatically upgraded by HCD (ANY/ONE/LOCAL_ONE → QUORUM).

> **`_id` handling in OpenSearch documents:** Every Data API document carries an `_id` field in `doc_json` (e.g. `{"_id":"p1","firstName":"Alice",...}`). When HCD unpacks `doc_json` it sends all fields — including `_id` — into the OpenSearch document body. OpenSearch **always rejects** `_id` as a body field because it is a reserved metadata field handled at the transport layer; no mapping can override this. The workaround is `propertiesFromJsonFields`, which acts as an allow-list that excludes `_id`. The resulting OpenSearch document shape is therefore:
> ```json
> {
>   "_index": "searchdemo-persons",
>   "_id":    "{_0=1, _1=p5}",
>   "_score": 1.0,
>   "_source": {
>     "firstName": "Diana",
>     "lastName":  "Brown",
>     "city":      "London",
>     "age":       51,
>     "key":       { "_0": 1, "_1": "p5" }
>   }
> }
> ```
> - The OpenSearch `_id` (metadata) is the **Cassandra composite primary key** in `{_0=<partitionKey>, _1=<clusteringKey>}` format — this is what HCD uses to route updates and deletes correctly.
> - The `key` field in `_source` is the decoded Cassandra primary key included automatically by HCD.
> - The user document fields (`firstName`, `lastName`, `city`, `age`) are present because they were listed in `propertiesFromJsonFields`.
> - The original Data API `_id` (`"p5"`) is **not** present in `_source` — it is intentionally excluded by the `propertiesFromJsonFields` allow-list.

### 3.1.1 Handling EJSON Extension Types in Collections (`$date`, `$binary`, `$uuid`, `$objectId`)

Data API collections use **EJSON extension types** to retain client-side typing when storing dates, binaries, UUIDs, and ObjectIds inside `doc_json TEXT`. Documents store these as single-entry JSON objects:
- Date: `{"publishedAt": {"$date": 1730419200000}}`
- Binary: `{"avatar": {"$binary": "iVBORw0KGgo..."}}`
- UUID: `{"userId": {"$uuid": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}}`
- ObjectId: `{"refId": {"$objectId": "507f1f77bcf86cd799439011"}}`

**How HCD unpacks EJSON fields:**
When HCD's `unpackJsonFields: 'doc_json'` extracts these properties, it treats them as generic JSON objects. The OpenSearch `_source` document receives the object structure verbatim:
```json
"_source": {
  "title": "Introduction to OpenSearch",
  "publishedAt": { "$date": 1730419200000 },
  "avatar": { "$binary": "iVBORw0KGgo..." },
  "userId": { "$uuid": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" }
}
```

If OpenSearch receives a top-level mapping like `"publishedAt": {"type": "date"}`, OpenSearch will reject the write with `mapper_parsing_exception: failed to parse field [publishedAt] of type [date] ... cannot parse object as date`.

**The Solution (Nested Sub-Field Mapping):**
For collections storing EJSON types, OpenSearch mappings define the nested property directly:
- **Date (`$date`):** `"publishedAt": { "properties": { "$date": { "type": "date" } } }`
- **Binary (`$binary`):** `"avatar": { "properties": { "$binary": { "type": "binary" } } }`
- **UUID (`$uuid`):** `"userId": { "properties": { "$uuid": { "type": "keyword" } } }`
- **ObjectId (`$objectId`):** `"refId": { "properties": { "$objectId": { "type": "keyword" } } }`

Queries in `$openSearch` and aggregations in `searchAggregate` then target the dot-path:
- `$openSearch` filter: `{"range": {"publishedAt.$date": {"gte": 1730419200000}}}`
- `searchAggregate`: `{"date_histogram": {"field": "publishedAt.$date", "calendar_interval": "month"}}`

*(Note: If documents store plain scalar ISO-8601 strings, e.g. `"publishedAt": "2024-11-01T00:00:00Z"`, then `"publishedAt": {"type": "date"}` is used directly. For CQL **Tables** (§5), Cassandra stores native CQL types (`timestamp`, `blob`, `uuid`), which HCD unpacks as scalar values, so Tables always map `"published_at": {"type": "date"}` and query `"published_at"` directly.)*

### 3.1.2 Special Characters in Property Names (Dots, Ampersands, Dollar Signs)

When document property names contain special characters, the interaction between Data API JSON shredding, CQL `WITH OPTIONS`, and OpenSearch mapping semantics must be handled consistently:

1. **Ampersand (`&`) and Special Symbols (`-`, `_`, `@`, `/`, `#`) in Property Names:**
   - **Examples:** `"r&d"`, `"author&co"`, `"tag-name"`, `"user_email@domain"`.
   - **Mapping:** OpenSearch natively allows UTF-8 property names containing `&`, `-`, `_`, `@`, etc. Top-level mappings in `createCollection` / `alterCollection` map these directly:
     ```json
     "mappings": {
       "r&d": { "type": "keyword" },
       "author&co": { "type": "text", "analyzer": "english" }
     }
     ```
   - **CQL Serialisation:** In the emitted CQL, `propertiesFromJsonFields` comma-separates them (`'title,r&d,author&co'`) and `customMappingsJson` safely JSON-encodes them (`'{"properties":{"_id":{"type":"keyword"},"r&d":{"type":"keyword"},"author&co":{"type":"text"}}}'`).
   - **Full-Text Analysis Note:** In OpenSearch, standard text analyzers (e.g. `standard`, `english`) treat `&` and punctuation as word delimiters. If exact string matching is required for strings like `"R&D"` or `"AT&T"`, use `type: "keyword"` or multi-fields.

2. **Dot (`.`) in Property Names vs. Nested Paths:**
   - **Examples:** `"metrics.cpu"` vs. nested object `{"metrics": {"cpu": 95.5}}`.
   - **OpenSearch Mapping Behavior:** In OpenSearch, defining `"metrics.cpu": {"type": "float"}` in mapping properties is automatically expanded into an object hierarchy: `{"metrics": {"properties": {"cpu": {"type": "float"}}}}`.
   - **Collection Best Practice:** Users should declare nested object mappings using the explicit `properties` or `object` structure (`"metrics": {"properties": {"cpu": {"type": "float"}}}`). In `propertiesFromJsonFields`, HCD extracts the top-level property name (`metrics`).
   - **Querying:** In `$openSearch` queries, dot notation (`"metrics.cpu"`) traverses into the nested JSON object seamlessly.

3. **Dollar Sign (`$`) in Property Names:**
   - **Top-Level Constraint:** In Data API collections, top-level property names starting with `$` are reserved for JSON API operators (`$openSearch`, `$gt`, `$exists`, etc.) and cannot be used as user-defined field names.
   - **EJSON Extension Subfields:** Within nested wrapper objects, `$` is reserved for EJSON type identifiers (`$date`, `$binary`, `$uuid`, `$objectId`).
   - **Mapping & Querying:** EJSON wrappers are mapped as nested properties (e.g. `"publishedAt": {"properties": {"$date": {"type": "date"}}}`) and queried using dot paths (`"publishedAt.$date"`), as detailed in §3.1.1.

> **Diagram 2a — Write path.** Each row holds ~3 steps. The flow zigzags top-to-bottom so every node is readable at normal zoom.

```mermaid
flowchart TD
    %% Row 1
    A["CQL Write\nINSERT / UPDATE / DELETE"]
    -->B["OpenSearchMutatorInterceptor\nhooks into Cassandra write path"]
    -->C["Consistency Level upgrade\nANY / ONE / LOCAL_ONE → QUORUM"]

    %% Row 2
    C-->D["Read-back at QUORUM\nfetches the complete row"]
    -->E["ReplicatorStorage — WAL\nin-memory memtable\nspills to disk on overflow"]
    -->F["BulkProcessor\nbatches up to 1000 docs\nflushes every 1 second"]

    %% Row 3
    F-->|"POST /_bulk"|G["OpenSearch\nBulk Index API"]
    -->H["Callback\nSuccess or Failure"]
```

---

> **Diagram 2b — Read path.** Each row holds ~3 steps. The `expr(...)` cycle between `OpenSearchQueryHandler` and OpenSearch is shown explicitly.

```mermaid
flowchart TD
    %% Row 1
    A["Data API\nfind with filter.$openSearch"]
    -->B["CQL\nSELECT * FROM table\nWHERE expr(indexName, dsl)"]
    -->C["OpenSearchQueryHandler\nintercepts expr() predicate"]

    %% Row 2
    C-->|"search DSL"|D["OpenSearch\nPOST /index/_search"]
    -->|"matching doc PKs"|E["Cassandra\nrow fetch by primary key"]

    %% Row 3
    E-->F["Data API\ndeserialise doc_json\napply projection"]
    -->G["Response\ndocuments returned to client"]
```

---

## 4. Impacted Commands — Collections

---

### 4.1 `createCollection` — new `openSearch` option block

#### 4.1.1 Goal

`createCollection` gains a new optional `openSearch` block inside `options`. Setting `enabled: true` tells the Data API to create an OpenSearch replication index alongside the normal Cassandra collection table. From that point on, every document write is automatically mirrored to OpenSearch by HCD — no extra client action required. The block also controls which document fields are replicated, how the OS index is named, and whether the initial full-table backfill is skipped for large pre-existing data sets.

> **SAI index name vs. OS index name for collections.** When `openSearch.enabled: true` is set, the Data API emits a `CREATE CUSTOM INDEX` statement that carries **two separate names**:
> - The **SAI index name** (the Cassandra identifier, e.g. `hcd_mykeyspace_products`) is **always auto-generated** by the Data API as `hcd_<keyspace>_<collectionName>`. It cannot be chosen by the user. This name is what appears in CQL `expr(hcd_mykeyspace_products, ...)` queries and what HCD uses internally.
> - The **OS index name** (the OpenSearch index, e.g. `articles-full-text`) is set via the `openSearch.indexName` field and **can be chosen freely** by the user. When omitted it also defaults to `hcd_<keyspace>_<collectionName>`.
>
> This is the same freedom/constraint trade-off as the rest of collection settings: the user does not choose the SAI names of any of the collection's associated indexes (SAI, lexical, vector), but they do retain full control over the OS index name.

#### 4.1.2 How it works

```mermaid
sequenceDiagram
    participant APP as Application
    participant API as Data API
    participant CQL as Cassandra
    participant OMI as HCD Interceptor
    participant OS as OpenSearch

    Note over APP,OS: Collection creation

    APP->>API: createCollection name:products\nopenSearch.enabled:true\nindexing.allow:[name,price]
    API->>CQL: CREATE TABLE products (key, doc_json ...)
    CQL-->>API: OK
    API->>CQL: CREATE CUSTOM INDEX hcd_ks_products\n(SAI index name — auto-generated)\nON ks.products (doc_json)\nUSING OpenSearchIndex\nWITH OPTIONS indexName:hcd_ks_products\n(OS index name — defaults to same value)
    CQL-->>API: OK
    API-->>APP: {"status":{"ok":1}}

    Note over APP,OS: Every subsequent write is automatically mirrored

    APP->>API: insertOne {name:"Widget", price:9.99}
    API->>CQL: INSERT INTO products ...
    CQL->>OMI: intercept write — upgrade consistency to QUORUM
    OMI->>OS: index {_id:..., name:"Widget", price:9.99}
    OS-->>OMI: 200 OK
    CQL-->>API: acknowledged
    API-->>APP: {"status":{"insertedIds":[...]}}
```

#### 4.1.3 OpenSearch fields in the Data API Payload

> **Why `mappings` is always required for collections.** The HCD connector always needs three things: (1) `propertiesFromJsonFields` — the allow-list of top-level fields to extract from `doc_json`, critically **excluding `_id`**; (2) `customMappingsJson` — the explicit OS field mapping, because `applyDefaultSchema: 'true'` cannot work (the only Cassandra column is `doc_json TEXT` — no typed schema to derive from); (3) `applyCustomSchema: 'true'` + `applyDefaultSchema: 'false'`. The Data API derives both `propertiesFromJsonFields` (the top-level keys of `mappings`) and `customMappingsJson` from the single `mappings` field. `customMappingsJson` is built by wrapping `mappings` under `{"properties":{...}}` and prepending `"_id":{"type":"keyword"}` as the first property, then serialising the whole object to a JSON string. Omitting `mappings` returns `OPEN_SEARCH_MISSING_FIELD_MAPPINGS`.

| Field | Required | Default | Description |
|---|---|---|---|
| `enabled` | **yes** | — | `true` activates OpenSearch replication. `false` (or omitting the block) is a no-op — all other fields (`indexName`, `numShards`, `numReplicas`, `mappings`) **must be omitted** when `enabled: false`; providing any of them returns `INVALID_CREATE_COLLECTION_OPTIONS` naming the specific offending field. |
| `indexName` | no | `hcd_<keyspace>_<collectionName>` | **OS index name** — the name of the OpenSearch index (passed as `'indexName'` in `WITH OPTIONS`). Leave unset to use the auto-generated default. Note: the **SAI index name** (the Cassandra custom index identifier used in `expr(...)`) is always auto-generated as `hcd_<keyspace>_<collectionName>` and cannot be configured here. |
| `numShards` | no | *(HCD default)* | Number of primary shards for the OS index. When omitted, HCD uses its own default (currently `1`). |
| `numReplicas` | no | *(HCD default)* | Number of replicas for the OS index (analogous to Cassandra replication factor). When omitted, HCD uses its own default. |
| `mappings` | **yes** | — | Map of **top-level** document field name → OpenSearch field definition. The Data API derives `propertiesFromJsonFields` (the key set) and `customMappingsJson` (built as `{"properties":{"_id":{"type":"keyword"}, ...mappings}}` serialised to a JSON string) from this single field. **`_id` must never appear as a key** — it is a reserved OpenSearch metadata field and is injected automatically. For nested objects, use `{"type":"object"}` or a `{"properties":{...}}` block (HCD passes `customMappingsJson` verbatim to the OpenSearch Create Index API — confirmed). See the type reference below. |

**OpenSearch field type reference for collections**

| Type | Use for | Example |
|---|---|---|
| `text` | Full-text search with analysis (tokenised, lowercased) | `{"type":"text"}` or `{"type":"text","analyzer":"english"}` |
| `keyword` | Exact match, sorting, aggregations — short strings, enums, IDs | `{"type":"keyword"}` |
| `integer` | 32-bit whole numbers | `{"type":"integer"}` |
| `long` | 64-bit whole numbers | `{"type":"long"}` |
| `float` | 32-bit decimals | `{"type":"float"}` |
| `double` | 64-bit decimals | `{"type":"double"}` |
| `boolean` | `true` / `false` | `{"type":"boolean"}` |
| `date` (plain scalar string) | Plain ISO-8601 string in JSON (e.g. `"2026-03-30T12:00:00Z"`) | `{"type":"date"}` |
| `$date` (EJSON object) | Stored as `{"$date": 123...}` — mapped via nested property (see §3.1.1) | `{"properties":{"$date":{"type":"date"}}}` |
| `$binary` (EJSON object) | Stored as `{"$binary": "..."}` — mapped via nested property | `{"properties":{"$binary":{"type":"binary"}}}` |
| `$uuid` (EJSON object) | Stored as `{"$uuid": "..."}` — mapped via nested property | `{"properties":{"$uuid":{"type":"keyword"}}}` |
| `$objectId` (EJSON object) | Stored as `{"$objectId": "..."}` — mapped via nested property | `{"properties":{"$objectId":{"type":"keyword"}}}` |
| `object` | Nested JSON object — all sub-fields automatically indexed and queryable as `parent.child` in the DSL | `{"type":"object"}` or `{"properties":{...}}` |
| `nested` | Array of objects where sub-fields must be queried as independent units (avoids cross-element matches) | `{"type":"nested"}` |
| `knn_vector` | Dense vector for OpenSearch k-NN (advanced — see §4.4.4) | `{"type":"knn_vector","dimension":1536}` |

> **`object` — two equivalent syntaxes, both confirmed working.** HCD passes `customMappingsJson` verbatim to the OpenSearch Create Index API, so both forms work:
> - **Simple:** `"address": {"type": "object"}` — OpenSearch auto-detects sub-field types from the first document. Sub-fields are immediately queryable as `address.city`, `address.zip`, etc.
> - **With explicit sub-field types:** `"address": {"properties": {"city": {"type": "keyword"}, "zip": {"type": "keyword"}}}` — pins sub-field types precisely. `"type": "object"` is implicit when `"properties"` is present. Recommended when you need exact-match (`keyword`) vs full-text (`text`) control.

> **`nested` — use only for arrays of objects.** If `reviews` is `[{"score": 5, "author": "Alice"}, {"score": 3, "author": "Bob"}]` and you need a query to match only when *the same review* satisfies both conditions, use `nested`. Without it, OpenSearch flattens all values across array elements. For most collection use cases `object` is sufficient.

> **Constraint — `indexing.deny` is rejected:** `openSearch.enabled: true` cannot be combined with `indexing.deny`. This is a **technical constraint** — `propertiesFromJsonFields` is inherently an allow-list with no deny-list equivalent. Use `indexing.allow` or `openSearch.mappings` to control replication scope. Violating this rule returns `OPEN_SEARCH_INCOMPATIBLE_WITH_DENY_LIST`.

> **Constraint — `indexing.allow` filters the OpenSearch mapping:** When `indexing.allow` is also present, the Data API automatically filters `propertiesFromJsonFields` and `customMappingsJson` to the **intersection** of the `openSearch.mappings` keys and the `indexing.allow` field list. Fields listed in `openSearch.mappings` that are NOT in `indexing.allow` are silently dropped from the OpenSearch replication. This ensures that a field excluded from SAI indexing is also excluded from OpenSearch replication. If the intersection is empty (no mapping key survives the allow-list), the request is rejected with `OPEN_SEARCH_MISSING_FIELD_MAPPINGS`.

**Relationship to other search capabilities**

The `openSearch` block is a deliberate, explicit gate — the same model as hybrid, not lexical. It must be declared at creation time; using `$openSearch` on a collection that was not created with `openSearch.enabled: true` returns `OPEN_SEARCH_NOT_ENABLED`.

| Capability | Declared at creation | Search operator | Gated |
|---|---|---|---|
| Lexical (SAI text index) | `lexical.enabled: true` | `$match` | No |
| Hybrid (vector + lexical) | `vector` + `lexical` both present | `$hybrid` | Yes |
| OpenSearch replication | `openSearch.enabled: true` | `$openSearch` | Yes |

`openSearch` can be combined with `lexical`, `vector`, or both at creation time — each search operator remains independent and exclusive per query.

#### 4.1.4 Sample payloads

> **Every `createCollection` payload with `openSearch.enabled: true` must include `mappings`.** This is always required — see §4.1.3 for the reasoning.

---

**Minimal — basic fields, default OS index name:**
```json
{
  "createCollection": {
    "name": "persons",
    "options": {
      "openSearch": {
        "enabled": true,
        "mappings": {
          "firstName": { "type": "text" },
          "lastName":  { "type": "text" },
          "city":      { "type": "text" },
          "age":       { "type": "integer" }
        }
      }
    }
  }
}
```
→ CQL emitted (SAI index name and OS index name both default to `hcd_mykeyspace_persons`):
```cql
CREATE CUSTOM INDEX IF NOT EXISTS hcd_mykeyspace_persons
ON mykeyspace.persons (doc_json)
USING 'OpenSearchIndex'
WITH OPTIONS = {
  'indexName':               'hcd_mykeyspace_persons',
  'createIndexIfNotExists':  'true',
  'unpackJsonFields':        'doc_json',
  'applyDefaultSchema':      'false',
  'applyCustomSchema':       'true',
  'customMappingsJson':      '{"properties":{"_id":{"type":"keyword"},"firstName":{"type":"text"},"lastName":{"type":"text"},"city":{"type":"text"},"age":{"type":"integer"}}}',
  'propertiesFromJsonFields':'firstName,lastName,city,age'
};
```

> The Data API derives `propertiesFromJsonFields` from the keys of `mappings` (`firstName,lastName,city,age`) and builds `customMappingsJson` by wrapping `mappings` under `{"properties":{...}}` with `_id` prepended as the first entry, then serialising to a JSON string.

---

**With explicit OS index name and English text analyzer:**
```json
{
  "createCollection": {
    "name": "articles",
    "options": {
      "openSearch": {
        "enabled": true,
        "indexName": "articles-full-text",
        "mappings": {
          "title":       { "type": "text", "analyzer": "english" },
          "body":        { "type": "text", "analyzer": "english" },
          "author":      { "type": "keyword" },
          "publishedAt": { "properties": { "$date": { "type": "date" } } }
        }
      }
    }
  }
}
```

---

**`indexing.allow` + `openSearch.mappings` — fields filtered to the intersection:**
```json
{
  "createCollection": {
    "name": "articles",
    "options": {
      "indexing": {
        "allow": ["category", "price", "publishedAt", "title", "author"]
      },
      "openSearch": {
        "enabled": true,
        "indexName": "articles-full-text",
        "mappings": {
          "title":  { "type": "text", "analyzer": "english" },
          "body":   { "type": "text", "analyzer": "english" },
          "author": { "type": "keyword" }
        }
      }
    }
  }
}
```

> `category`, `price`, `publishedAt`, `title`, and `author` are SAI-indexed. `title`, `body`, and `author` are declared in `openSearch.mappings`. The intersection is `title` and `author` — only those two fields are passed to `propertiesFromJsonFields` and included in `customMappingsJson`. `body` is dropped from the OpenSearch mapping because it is not in `indexing.allow`. The CQL emitted is:
> ```cql
> CREATE CUSTOM INDEX IF NOT EXISTS hcd_mykeyspace_articles
> ON mykeyspace.articles (doc_json)
> USING 'OpenSearchIndex'
> WITH OPTIONS = {
>   'indexName':               'articles-full-text',
>   'createIndexIfNotExists':  'true',
>   'unpackJsonFields':        'doc_json',
>   'applyDefaultSchema':      'false',
>   'applyCustomSchema':       'true',
>   'customMappingsJson':      '{"properties":{"_id":{"type":"keyword"},"title":{"type":"text","analyzer":"english"},"author":{"type":"keyword"}}}',
>   'propertiesFromJsonFields':'title,author'
> };
> ```

---

**With a nested `object` field — dynamic sub-field detection:**
```json
{
  "createCollection": {
    "name": "orders",
    "options": {
      "openSearch": {
        "enabled": true,
        "mappings": {
          "customerName": { "type": "keyword" },
          "status":       { "type": "keyword" },
          "address":      { "type": "object" }
        }
      }
    }
  }
}
```

> `address` is declared as `object` — OpenSearch auto-detects sub-field types from the first document and makes them queryable as `address.city`, `address.zip`, etc. in the `$openSearch` DSL.

---

**With a nested `object` field — explicit sub-field types:**
```json
{
  "createCollection": {
    "name": "orders",
    "options": {
      "openSearch": {
        "enabled": true,
        "mappings": {
          "customerName": { "type": "keyword" },
          "status":       { "type": "keyword" },
          "address": {
            "properties": {
              "city":    { "type": "keyword" },
              "country": { "type": "keyword" },
              "zip":     { "type": "keyword" }
            }
          }
        }
      }
    }
  }
}
```

> `"type": "object"` is implicit when `"properties"` is present. This form pins sub-field types precisely — use it when you need exact-match (`keyword`) vs full-text (`text`) control on sub-fields.

---

**Combined with vector search — ANN and full-text search on the same collection:**
```json
{
  "createCollection": {
    "name": "knowledge_base",
    "options": {
      "vector": {
        "dimension": 1536,
        "metric": "cosine",
        "service": {
          "provider": "openai",
          "modelName": "text-embedding-ada-002"
        }
      },
      "openSearch": {
        "enabled": true,
        "mappings": {
          "title":   { "type": "text", "analyzer": "english" },
          "content": { "type": "text", "analyzer": "english" },
          "tags":    { "type": "keyword" },
          "source":  { "type": "keyword" }
        }
      }
    }
  }
}
```

---

**Error — `mappings` missing (always required):**
```json
{
  "createCollection": {
    "name": "products",
    "options": {
      "openSearch": {
        "enabled": true
      }
    }
  }
}
```
Response:
```json
{
  "errors": [{
    "message": "openSearch.mappings is required. There is no auto-inference path for collections.",
    "errorCode": "OPEN_SEARCH_MISSING_FIELD_MAPPINGS"
  }]
}
```

---

**Error — `_id` present in `mappings`:**
```json
{
  "createCollection": {
    "name": "products",
    "options": {
      "openSearch": {
        "enabled": true,
        "mappings": {
          "_id":   { "type": "keyword" },
          "name":  { "type": "text" },
          "price": { "type": "double" }
        }
      }
    }
  }
}
```
Response:
```json
{
  "errors": [{
    "message": "'_id' must not appear as a key in openSearch.mappings. It is a reserved OpenSearch metadata field and is injected automatically.",
    "errorCode": "OPEN_SEARCH_MISSING_FIELD_MAPPINGS"
  }]
}
```

---

**Error — `indexing.deny` combined with `openSearch.enabled: true` is rejected:**
```json
{
  "createCollection": {
    "name": "users",
    "options": {
      "indexing": {
        "deny": ["password_hash", "ssn"]
      },
      "openSearch": {
        "enabled": true,
        "mappings": {
          "username": { "type": "keyword" },
          "email":    { "type": "text" }
        }
      }
    }
  }
}
```
Response:
```json
{
  "errors": [{
    "message": "openSearch cannot be enabled when 'indexing.deny' is set. Use 'indexing.allow' or 'openSearch.mappings' to specify which fields to replicate.",
    "errorCode": "OPEN_SEARCH_INCOMPATIBLE_WITH_DENY_LIST"
  }]
}
```

---

**Error — `indexing.allow` present but intersection with `openSearch.mappings` keys is empty:**
```json
{
  "createCollection": {
    "name": "products",
    "options": {
      "indexing": {
        "allow": ["category", "price"]
      },
      "openSearch": {
        "enabled": true,
        "mappings": {
          "title":       { "type": "text", "analyzer": "english" },
          "description": { "type": "text", "analyzer": "english" }
        }
      }
    }
  }
}
```
Response:
```json
{
  "errors": [{
    "message": "openSearch.mappings contains no fields that are in 'indexing.allow'. The intersection of openSearch.mappings keys and indexing.allow is empty — no fields would be replicated to OpenSearch.",
    "errorCode": "OPEN_SEARCH_MISSING_FIELD_MAPPINGS"
  }]
}
```

---

**Error — `enabled: false` with `indexName` provided:**
```json
{
  "createCollection": {
    "name": "products",
    "options": {
      "openSearch": {
        "enabled": false,
        "indexName": "products-search"
      }
    }
  }
}
```
Response:
```json
{
  "errors": [{
    "message": "'createCollection' command option(s) invalid: 'openSearch' is disabled, but 'openSearch.indexName' was provided. When 'openSearch' is disabled, 'openSearch.indexName' must be omitted.",
    "errorCode": "INVALID_CREATE_COLLECTION_OPTIONS"
  }]
}
```

> Each offending field is reported separately. The same pattern applies to `numShards`, `numReplicas`, and `mappings` — the error message always names the specific field. This mirrors the behaviour of `lexical.enabled: false` with `lexical.analyzer` provided.

---

**Error — `enabled: false` with `mappings` provided:**
```json
{
  "createCollection": {
    "name": "products",
    "options": {
      "openSearch": {
        "enabled": false,
        "mappings": {
          "name": { "type": "text" }
        }
      }
    }
  }
}
```
Response:
```json
{
  "errors": [{
    "message": "'createCollection' command option(s) invalid: 'openSearch' is disabled, but 'openSearch.mappings' was provided. When 'openSearch' is disabled, 'openSearch.mappings' must be omitted.",
    "errorCode": "INVALID_CREATE_COLLECTION_OPTIONS"
  }]
}
```

---

#### 4.1.5 Implementation Tips (server)

**CQL emitted**

When `openSearch.enabled: true` the Data API executes a second CQL statement immediately after `CREATE TABLE`. The same five `WITH OPTIONS` keys are **always present** for every collection OpenSearch index — they are not conditional:

- `applyDefaultSchema: 'false'` — always. The only Cassandra column is `doc_json TEXT`; there is no typed schema to derive a mapping from.
- `applyCustomSchema: 'true'` + `customMappingsJson` — always. The Data API builds `customMappingsJson` by constructing `{"properties":{"_id":{"type":"keyword"}, <mappings entries>}}` and serialising the result to a JSON string. The `_id` entry is injected automatically as the first property — it must not appear in `mappings`.
- `propertiesFromJsonFields` — always. Built from the **effective field set** as a comma-separated string. When `indexing.allow` is also present, this is the **intersection** of `openSearch.mappings` top-level keys and the `indexing.allow` field list; when `indexing.allow` is absent it is simply the top-level keys of `openSearch.mappings`. `_id` is always excluded (validated at request time). An empty intersection is rejected with `OPEN_SEARCH_MISSING_FIELD_MAPPINGS`.
- `unpackJsonFields: 'doc_json'` — always. Required so HCD parses the JSON blob.

*Standard example — `persons` collection, default OS index name:*
```cql
-- SAI index name: hcd_mykeyspace_persons   (Cassandra identifier, used in expr())
-- OS index name:  hcd_mykeyspace_persons   (OpenSearch index — defaults to same value)
CREATE CUSTOM INDEX IF NOT EXISTS hcd_mykeyspace_persons
ON mykeyspace.persons (doc_json)
USING 'OpenSearchIndex'
WITH OPTIONS = {
  'indexName':               'hcd_mykeyspace_persons',
  'createIndexIfNotExists':  'true',
  'unpackJsonFields':        'doc_json',
  'applyDefaultSchema':      'false',
  'applyCustomSchema':       'true',
  'customMappingsJson':      '{"properties":{"_id":{"type":"keyword"},"firstName":{"type":"text"},"lastName":{"type":"text"},"city":{"type":"text"},"age":{"type":"integer"}}}',
  'propertiesFromJsonFields':'firstName,lastName,city,age'
};
```

*With explicit OS index name and English analyzer (OS index name `articles-full-text` differs from SAI index name `hcd_mykeyspace_articles`):*
```cql
-- SAI index name: hcd_mykeyspace_articles  (Cassandra identifier, used in expr())
-- OS index name:  articles-full-text        (OpenSearch index, set via 'indexName' in WITH OPTIONS)
CREATE CUSTOM INDEX IF NOT EXISTS hcd_mykeyspace_articles
ON mykeyspace.articles (doc_json)
USING 'OpenSearchIndex'
WITH OPTIONS = {
  'indexName':               'articles-full-text',
  'createIndexIfNotExists':  'true',
  'unpackJsonFields':        'doc_json',
  'applyDefaultSchema':      'false',
  'applyCustomSchema':       'true',
  'customMappingsJson':      '{"properties":{"_id":{"type":"keyword"},"title":{"type":"text","analyzer":"english"},"body":{"type":"text","analyzer":"english"},"author":{"type":"keyword"},"publishedAt":{"properties":{"$date":{"type":"date"}}}}}',
  'propertiesFromJsonFields':'title,body,author,publishedAt'
};
```

**`WITH OPTIONS` mapping rules**

> The `WITH OPTIONS` block carries both the **SAI index name** (implicit in the `CREATE CUSTOM INDEX` statement itself) and the **OS index name** (explicit as the `'indexName'` option). See §3 for the formal distinction.

| Condition | Options key added |
|---|---|
| **Always** | `indexName`, `createIndexIfNotExists: 'true'`, `unpackJsonFields: 'doc_json'`, `applyDefaultSchema: 'false'`, `applyCustomSchema: 'true'`, `customMappingsJson` ¹, `propertiesFromJsonFields` ² |
| `openSearch.numShards` set by user | `numShards: '<value>'` |
| `openSearch.numReplicas` set by user | `numReplicas: '<value>'` |

> ¹ `customMappingsJson` is always present. The Data API constructs `{"properties":{"_id":{"type":"keyword"}, <mappings entries>}}` and serialises it to a JSON string. The `{"properties":{...}}` wrapper is the OpenSearch index mapping envelope; `_id` is injected automatically as the first entry. The option ordering in the emitted CQL is: `indexName`, `createIndexIfNotExists`, `unpackJsonFields`, `applyDefaultSchema`, `applyCustomSchema`, `customMappingsJson`, `propertiesFromJsonFields`.

> ² `propertiesFromJsonFields` is always present. When `indexing.allow` is absent it equals the top-level keys of `mappings` (i.e. `Object.keys(mappings)`). When `indexing.allow` is present it equals `Object.keys(mappings).filter(k => indexing.allow.includes(k))` — the intersection. `_id` must never appear in either input — validated at request time. An empty intersection is rejected with `OPEN_SEARCH_MISSING_FIELD_MAPPINGS`. For **tables** (§4.3) the equivalent options are optional (HCD can use `applyDefaultSchema: 'true'`); for **collections** they are mandatory.

**Todo list**
1. Add `OpenSearchDesc` record to `CreateCollectionCommand.Options` — fields: `Boolean enabled` (required), `Integer numShards` (optional), `Integer numReplicas` (optional), `String indexName` (optional), `JsonNode mappings` (**required**).
2. Create `CollectionOpenSearchDef` record in `service/schema/collections` following `CollectionLexicalDef` pattern — includes `fromApiDesc()`, `toApiDesc()`, and a `SchemaDefaults` inner instance.
3. Create `CollectionOpenSearchDefSchemaFactory` following the lexical pattern.
4. Add `openSearchDef` field (`SchemaHolder<CollectionOpenSearchDef>`) to `CollectionSchemaObject`.
5. Add `CollectionSchemaVersion.V_3`; add `CollectionSettingsV3Reader` (extends V2Reader).
6. Update V1/V2 readers to return `SchemaDefaults.forPreRelease()` when `openSearch` key is absent.
7. Update `CreateCollectionCommandResolver` to: (a) reject `indexing.deny` + `openSearch.enabled: true` with `OPEN_SEARCH_INCOMPATIBLE_WITH_DENY_LIST`; (b) validate that `mappings` is present and non-empty (throw `OPEN_SEARCH_MISSING_FIELD_MAPPINGS` if absent); (c) validate that `_id` does not appear as a key in `mappings`; (d) when `indexing.allow` is present, compute the **effective field set** as `Object.keys(mappings).filter(k => indexing.allow.includes(k))` and reject with `OPEN_SEARCH_MISSING_FIELD_MAPPINGS` if the result is empty; (e) resolve `indexName` default; persist resolved values in comment JSON. — In `CollectionOpenSearchDef.fromApiDesc()`, when `enabled: false`, check each of `indexName`, `numShards`, `numReplicas`, and `mappings` individually and throw `INVALID_CREATE_COLLECTION_OPTIONS` naming the specific field (mirrors `lexical.enabled: false` + `analyzer` pattern).
8. Update `CreateCollectionOperation` to emit `CREATE CUSTOM INDEX … USING 'OpenSearchIndex'` after table creation: build `propertiesFromJsonFields` from the **effective field set** (intersection when `indexing.allow` is present, full `mappings` key set otherwise); build `customMappingsJson` by constructing `{"properties":{"_id":{"type":"keyword"}, ...<effective mappings entries>}}` and serialising to a JSON string; include all five mandatory options.
9. Update `CollectionSchemaFactory` to read/write the `openSearch` key in the comment JSON.

**Relevant context**
- Pattern: [`CollectionLexicalDef`](src/main/java/io/stargate/sgv2/jsonapi/service/schema/collections/CollectionLexicalDef.java), [`CollectionLexicalDefSchemaFactory`](src/main/java/io/stargate/sgv2/jsonapi/service/schema/collections/CollectionLexicalDefSchemaFactory.java)
- Command: [`CreateCollectionCommand`](src/main/java/io/stargate/sgv2/jsonapi/api/model/command/impl/CreateCollectionCommand.java)
- Resolver: [`CreateCollectionCommandResolver`](src/main/java/io/stargate/sgv2/jsonapi/service/resolver/CreateCollectionCommandResolver.java)
- Operation: [`CreateCollectionOperation`](src/main/java/io/stargate/sgv2/jsonapi/service/operation/collections/CreateCollectionOperation.java)
- Schema: [`CollectionSchemaObject`](src/main/java/io/stargate/sgv2/jsonapi/service/schema/collections/CollectionSchemaObject.java), [`CollectionSchemaVersion`](src/main/java/io/stargate/sgv2/jsonapi/service/schema/CollectionSchemaVersion.java)
- Readers: [`CollectionSettingsV1Reader`](src/main/java/io/stargate/sgv2/jsonapi/service/schema/collections/CollectionSettingsV1Reader.java), [`CollectionSettingsV2Reader`](src/main/java/io/stargate/sgv2/jsonapi/service/schema/collections/CollectionSettingsV2Reader.java)

#### 4.1.6 Implementation Tips (clients)

> Client SDK documentation: [HCD API Reference — Instantiate Client](https://docs.datastax.com/en/hyper-converged-database/2.0/api-reference/instantiate-client.html) and sub-pages.

**Java**
```java
// mappings — flat fields
CollectionOptions options = CollectionOptions.builder()
    .openSearch(OpenSearchOptions.builder()
        .enabled(true)
        .indexName("articles-full-text")
        .mappings(Map.of(
            "title",  OpenSearchFieldType.text("english"),
            "body",   OpenSearchFieldType.text("english"),
            "author", OpenSearchFieldType.keyword()
        ))
        .build())
    .build();
database.createCollection("articles", options);

// SAI indexing for filters, separate OpenSearch mappings for full-text
CollectionOptions mixed = CollectionOptions.builder()
    .indexing(IndexingOptions.builder()
        .allow("category", "price", "publishedAt")
        .build())
    .openSearch(OpenSearchOptions.builder()
        .enabled(true)
        .indexName("articles-full-text")
        .mappings(Map.of(
            "title",  OpenSearchFieldType.text("english"),
            "body",   OpenSearchFieldType.text("english"),
            "author", OpenSearchFieldType.keyword()
        ))
        .build())
    .build();
database.createCollection("articles", mixed);
```

**Python**
```python
from astrapy.info import (
    CollectionDefinition,
    CollectionOpenSearchOptions,
    CollectionIndexingOptions,
)

# mappings — using CollectionDefinition class
database.create_collection(
    "articles",
    definition=CollectionDefinition(
        open_search=CollectionOpenSearchOptions(
            enabled=True,
            index_name="articles-full-text",
            mappings={
                "title":  {"type": "text", "analyzer": "english"},
                "body":   {"type": "text", "analyzer": "english"},
                "author": {"type": "keyword"},
            },
        )
    ),
)

# SAI indexing for filters, separate OpenSearch mappings for full-text
database.create_collection(
    "articles",
    definition=CollectionDefinition(
        indexing=CollectionIndexingOptions(allow=["category", "price", "publishedAt"]),
        open_search=CollectionOpenSearchOptions(
            enabled=True,
            index_name="articles-full-text",
            mappings={
                "title":  {"type": "text", "analyzer": "english"},
                "body":   {"type": "text", "analyzer": "english"},
                "author": {"type": "keyword"},
            },
        ),
    ),
)
```

**TypeScript / Node.js**
```typescript
// mappings — flat fields
await db.createCollection("articles", {
  options: {
    openSearch: {
      enabled: true,
      indexName: "articles-full-text",
      mappings: {
        title:  { type: "text", analyzer: "english" },
        body:   { type: "text", analyzer: "english" },
        author: { type: "keyword" }
      }
    }
  }
});

// SAI indexing for filters, separate OpenSearch mappings for full-text
await db.createCollection("articles", {
  options: {
    indexing: { allow: ["category", "price", "publishedAt"] },
    openSearch: {
      enabled: true,
      indexName: "articles-full-text",
      mappings: {
        title:  { type: "text", analyzer: "english" },
        body:   { type: "text", analyzer: "english" },
        author: { type: "keyword" }
      }
    }
  }
});
```

---

### 4.2 `findCollections` — return `openSearch` block in `explain` response

#### 4.2.1 Goal

When `findCollections` is called with `options.explain: true`, the `openSearch` block is returned as part of each collection's definition:

- For collections where OpenSearch is enabled (`openSearch.enabled: true`), the full resolved configuration is returned — including `enabled: true`, `indexName`, `numShards`, `numReplicas`, and `mappings`.
- For collections where OpenSearch is disabled or not configured, the block returns `{"openSearch": {"enabled": false}}`, **mirroring the exact behavior of `lexical` and `rerank`** (`{"lexical": {"enabled": false}}`, `{"rerank": {"enabled": false}}`).

This allows callers and client SDKs to:
- inspect the resolved configuration — including the auto-generated OS index name — without touching the raw Cassandra table comment,
- reliably inspect `col.options.openSearch.enabled` (boolean `true` or `false`) without needing null guards or guessing whether the feature is supported,
- reconstruct the original creation payload for IaC or migration tooling, and
- let client libraries confirm whether OpenSearch is active before routing a `$openSearch` query.

One detail: `indexName` always returns the **resolved OS index name**. If the caller omitted it at creation time the Data API persisted the auto-generated default (`hcd_<keyspace>_<collectionName>`), and that is what is returned here. The **SAI index name** is not exposed in the `findCollections` response — it is an internal Cassandra identifier that clients do not need to interact with directly.

#### 4.2.2 How it works

```mermaid
sequenceDiagram
    participant APP as Application
    participant API as Data API
    participant CQL as Cassandra

    APP->>API: findCollections / options.explain: true
    API->>CQL: SELECT table_options FROM system_schema.tables
    CQL-->>API: table comment JSON per collection
    API->>API: CollectionSettingsV3Reader deserialises openSearch block<br/>resolves indexName if absent
    API-->>APP: collections[] each V3+enabled collection<br/>includes openSearch block with enabled, indexName, ...
```

#### 4.2.3 OpenSearch fields in the Data API Payload

| Field | Always present | Omitted when | Notes |
|---|---|---|---|
| `enabled` | ✅ yes | — | Boolean `true` when active, `false` when disabled or not configured. |
| `indexName` | When `enabled: true` | When `enabled: false` | **Resolved OS index name** — never null when enabled. This is the OpenSearch index name (the `'indexName'` `WITH OPTIONS` value). The SAI index name (the Cassandra custom index identifier) is not returned here. |
| `numShards` | When `enabled: true` | When `enabled: false` | Persisted at creation time; always returned when enabled. |
| `numReplicas` | When `enabled: true` | When `enabled: false` | Persisted at creation time; always returned when enabled. |
| `mappings` | When `enabled: true` | When `enabled: false` | The `mappings` object as provided at creation time; always persisted and always returned when enabled. |

#### 4.2.4 Sample payloads

**Request — `explain: true` is required to get the `openSearch` block:**
```json
{
  "findCollections": {
    "options": { "explain": true }
  }
}
```

---

**Response — articles collection with English analyzer:**
```json
{
  "status": {
    "collections": [
      {
        "name": "articles",
        "options": {
          "openSearch": {
            "enabled": true,
            "numShards": 1,
            "numReplicas": 1,
            "indexName": "articles-full-text",
            "mappings": {
              "title":       { "type": "text", "analyzer": "english" },
              "body":        { "type": "text", "analyzer": "english" },
              "author":      { "type": "keyword" },
              "publishedAt": { "type": "date" }
            }
          }
        }
      }
    ]
  }
}
```

> All fields are always round-tripped: `indexName`, `numShards`, `numReplicas`, and `mappings` are always present when `enabled: true`.

---

**Response — collection with `indexing.allow` (SAI) and separate `openSearch.mappings`:**
```json
{
  "status": {
    "collections": [
      {
        "name": "articles",
        "options": {
          "indexing": { "allow": ["category", "price", "publishedAt"] },
          "openSearch": {
            "enabled": true,
            "numShards": 1,
            "numReplicas": 1,
            "indexName": "articles-full-text",
            "mappings": {
              "title":  { "type": "text", "analyzer": "english" },
              "body":   { "type": "text", "analyzer": "english" },
              "author": { "type": "keyword" }
            }
          }
        }
      }
    ]
  }
}
```

> `indexing.allow` and `openSearch.mappings` are independent and both round-tripped as provided.

---

**Response — mixed list: one OpenSearch-enabled collection, two without:**
```json
{
  "status": {
    "collections": [
      {
        "name": "articles",
        "options": {
          "openSearch": {
            "enabled": true,
            "numShards": 1,
            "numReplicas": 1,
            "indexName": "hcd_my_app_articles",
            "mappings": {
              "abstract":        { "type": "text", "analyzer": "english" },
              "longDescription": { "type": "text" }
            }
          }
        }
      },
      {
        "name": "users",
        "options": {
          "indexing": { "deny": ["password_hash"] },
          "lexical": { "enabled": true, "analyzer": "standard" },
          "rerank": { "enabled": true, "service": { "provider": "nvidia", "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2" } },
          "openSearch": { "enabled": false }
        }
      },
      {
        "name": "sessions",
        "options": {
          "lexical": { "enabled": true, "analyzer": "standard" },
          "rerank": { "enabled": true, "service": { "provider": "nvidia", "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2" } },
          "openSearch": { "enabled": false }
        }
      }
    ]
  }
}
```

> `users` and `sessions` explicitly return `"openSearch": {"enabled": false}` — matching the behavior of `lexical` and `rerank`.

---

**Response — `explain: false` (default) — `openSearch` block is never returned regardless of schema version:**
```json
{
  "status": {
    "collections": ["articles", "users", "sessions"]
  }
}
```

> Without `explain: true` the response is a flat name list. The `openSearch` block is never included in this form.

---

**Response — V1/V2 collection (no schema upgrade performed) — `openSearch` absent:**
```json
{
  "status": {
    "collections": [
      {
        "name": "legacy_docs",
        "options": {
          "vector": {
            "dimension": 1536,
            "metric": "cosine"
          }
        }
      }
    ]
  }
}
```

> A collection created before V3 has no `openSearch` node in its stored comment. `CollectionSettingsV3Reader` is not invoked; V1/V2 readers produce no `openSearch` field. Response is identical to today's `findCollections` behaviour.

#### 4.2.5 Implementation Tips (server)

**Behaviour rules**
- `findCollections` with `explain: true` includes an `openSearch` key for all collections:
  - When enabled: `{"enabled": true, "indexName": "...", "numShards": 1, "numReplicas": 1, "mappings": {...}}`.
  - When disabled: `{"enabled": false}` (matching `lexical` and `rerank`).
- The returned object always contains `enabled`. When `enabled == true`, `indexName`, `numShards`, `numReplicas`, and `mappings` are also present.
- Collections with schema V1 or V2 return `{"enabled": false}` (via `CollectionOpenSearchDef.SCHEMA_DEFAULTS.forPreRelease()`).
- Schema read-back: `CollectionSettingsV3Reader` must (1) read `schema.openSearch`; (2) if missing or `enabled == false` produce a disabled def; (3) if `enabled == true` populate all fields — if `indexName` is null in the stored comment, **throw `OPEN_SEARCH_CORRUPT_SCHEMA`** rather than silently recomputing the default.

**Todo list**
1. Verify `CollectionSettingsV3Reader` stores `indexName`, `numShards`, `numReplicas`, and `mappings` as resolved values at write time.
2. Update `CollectionSchemaObject.collectionSettingToCreateCollectionCommand()` to include `collectionSetting.openSearchDef().toApiDesc()` directly.
3. Unit tests: V3 full config round-trips (`enabled: true`); V3 `enabled: false` returns `{"enabled": false}`; V2 returns `{"enabled": false}`.

#### 4.2.6 Implementation Tips (clients)

> Client SDK documentation: [HCD API Reference — Instantiate Client](https://docs.datastax.com/en/hyper-converged-database/2.0/api-reference/instantiate-client.html) and sub-pages.

**Java**
```java
List<CollectionInfo> collections = database.listCollections(
    ListCollectionsOptions.builder().explain(true).build()
);
collections.forEach(info -> {
    OpenSearchOptions os = info.getOptions().getOpenSearch();
    if (os != null) {
        System.out.println(info.getName() + " → " + os.getIndexName());
        System.out.println("  mappings: " + os.getMappings());
    }
});
```

**Python**

> `list_collections()` — called without arguments — returns full `CollectionDefinition` instances (it is the explain-mode variant; `list_collection_names()` returns only names). The returned objects are class instances, not dictionaries: use attribute access (`.open_search.index_name`) rather than dictionary access (`["openSearch"]["indexName"]`). The `open_search` attribute is either `None` or an instance of `CollectionOpenSearchOptions`.

```python
from astrapy.info import CollectionDefinition, CollectionOpenSearchOptions

for col_def in database.list_collections():
    # col_def is a CollectionDefinition; col_def.open_search is None or CollectionOpenSearchOptions
    os_opts: CollectionOpenSearchOptions | None = col_def.open_search
    if os_opts is not None:
        print(f"{col_def.name} → {os_opts.index_name}")
        print(f"  mappings: {os_opts.mappings}")
```

**TypeScript / Node.js**
```typescript
const collections = await db.listCollections({ explain: true });
for (const col of collections) {
  if (col.options?.openSearch) {
    console.log(`${col.name} → ${col.options.openSearch.indexName}`);
    console.log(`  mappings: ${JSON.stringify(col.options.openSearch.mappings)}`);
  }
}
```

---

### 4.3 `alterCollection` — add or drop the OpenSearch index on an existing collection

#### 4.3.1 Goal

`alterCollection` is extended with two new operations — `addOpenSearch` and `dropOpenSearch` — that allow callers to attach or remove an OpenSearch replication index on a collection that already exists, **without recreating the collection**. This is the counterpart to the `openSearch` block in `createCollection`: anything that can be expressed at creation time can also be expressed after the fact via `alterCollection`.

> **OpenSearch replication is mutable after collection creation.** Unlike `lexical` or `rerank` (which are fixed at creation), the `openSearch` block is explicitly designed to be added and removed post-creation via `alterCollection`. Each operation issues a `CREATE CUSTOM INDEX` or `DROP INDEX` on the underlying Cassandra table and updates the schema version stored in the table comment.

After `addOpenSearch` completes:
- `findCollections` with `explain: true` returns the `openSearch` block with all resolved fields (`enabled: true`, `indexName`, `numShards`, `numReplicas`, `mappings`).
- `$openSearch` queries on the collection are accepted.

After `dropOpenSearch` completes:
- `findCollections` with `explain: true` returns `{"openSearch": {"enabled": false}}`.
- `$openSearch` queries on the collection return `OPEN_SEARCH_NOT_ENABLED_FOR_COLLECTION`.

#### 4.3.2 How it works

```mermaid
sequenceDiagram
    participant APP as Application
    participant API as Data API
    participant CQL as Cassandra

    APP->>API: alterCollection\noperation: {addOpenSearch: {enabled: true, mappings: {...}}}
    API->>API: validate no existing OpenSearch index on collection\nbuild saiIndexName = hcd_<ks>_<collection>\nresolve indexName default if absent\nwrap mappings under {properties:{...}}\nserialise to customMappingsJson
    API->>CQL: CREATE CUSTOM INDEX IF NOT EXISTS hcd_<ks>_<collection>\nON <ks>.<collection> (doc_json)\nUSING 'OpenSearchIndex'\nWITH OPTIONS = {'indexName': '...', 'unpackJsonFields': 'doc_json', ...}
    CQL-->>API: OK
    API->>CQL: UPDATE system_schema.tables\n(write updated comment JSON with openSearch block, bump schema version to V_3)
    CQL-->>API: OK
    API-->>APP: {"status":{"ok":1}}
```

```mermaid
sequenceDiagram
    participant APP as Application
    participant API as Data API
    participant CQL as Cassandra

    APP->>API: alterCollection\noperation: {dropOpenSearch: {}}
    API->>API: look up existing OpenSearch index on collection\nresolve saiIndexName = hcd_<ks>_<collection>
    API->>CQL: DROP INDEX IF EXISTS hcd_<ks>_<collection>
    CQL-->>API: OK
    API->>CQL: UPDATE system_schema.tables\n(remove openSearch block from comment JSON, downgrade schema if needed)
    CQL-->>API: OK
    API-->>APP: {"status":{"ok":1}}
```

#### 4.3.3 OpenSearch fields in the Data API Payload

**`addOpenSearch` operation fields:**

| Field | Required | Default | Description |
|---|---|---|---|
| `operation.addOpenSearch.enabled` | **yes** | — | Must be `true`. |
| `operation.addOpenSearch.indexName` | no | `hcd_<keyspace>_<collectionName>` | **OS index name** — the OpenSearch index name. Defaults to `hcd_<keyspace>_<collectionName>` when absent. |
| `operation.addOpenSearch.numShards` | no | *(HCD default)* | Number of primary shards. |
| `operation.addOpenSearch.numReplicas` | no | *(HCD default)* | Number of replicas. |
| `operation.addOpenSearch.mappings` | **yes** | — | Flat field-name → OpenSearch field definition map. Required (same constraint as `createCollection`). Must be a non-empty object and must not contain `_id`. |

**`dropOpenSearch` operation fields:**

| Field | Required | Description |
|---|---|---|
| `operation.dropOpenSearch` | **yes** | Empty object `{}` — no parameters needed. The SAI index name is always auto-generated and unambiguous. |

#### 4.3.4 Sample payloads

**Add OpenSearch to an existing collection:**
```json
{
  "alterCollection": {
    "operation": {
      "addOpenSearch": {
        "enabled": true,
        "indexName": "articles-full-text",
        "numShards": 1,
        "numReplicas": 1,
        "mappings": {
          "title":  { "type": "text", "analyzer": "english" },
          "body":   { "type": "text", "analyzer": "english" },
          "author": { "type": "keyword" }
        }
      }
    }
  }
}
```
→ CQL emitted:
```cql
-- SAI index name: hcd_mykeyspace_articles  (always auto-generated)
-- OS index name:  articles-full-text       (user-supplied via indexName)
CREATE CUSTOM INDEX IF NOT EXISTS hcd_mykeyspace_articles
ON mykeyspace.articles (doc_json)
USING 'OpenSearchIndex'
WITH OPTIONS = {
  'createIndexIfNotExists': 'true',
  'indexName':              'articles-full-text',
  'unpackJsonFields':       'doc_json',
  'propertiesFromJsonFields': 'title,body,author',
  'applyCustomSchema':      'true',
  'applyDefaultSchema':     'false',
  'customMappingsJson':     '{"properties":{"title":{"type":"text","analyzer":"english"},"body":{"type":"text","analyzer":"english"},"author":{"type":"keyword"}}}'
};
```
Schema comment is updated to `V_3` and the `openSearch` block is persisted. `findCollections explain: true` now returns the `openSearch` block.

---

**Add OpenSearch — minimal (default OS index name, default shard/replica counts):**
```json
{
  "alterCollection": {
    "operation": {
      "addOpenSearch": {
        "enabled": true,
        "mappings": {
          "firstName": { "type": "text" },
          "lastName":  { "type": "keyword" }
        }
      }
    }
  }
}
```

---

**Drop OpenSearch from an existing collection:**
```json
{
  "alterCollection": {
    "operation": {
      "dropOpenSearch": {}
    }
  }
}
```
→ CQL emitted:
```cql
DROP INDEX IF EXISTS hcd_mykeyspace_articles;
```
Schema comment is updated: `openSearch` block removed, schema version downgraded if no other V_3 features require it.

---

**Error — `addOpenSearch` on a collection that already has OpenSearch enabled:**
```json
{
  "errors": [{
    "message": "The collection 'articles' already has an OpenSearch index configured.",
    "errorCode": "OPEN_SEARCH_INDEX_ALREADY_EXISTS"
  }]
}
```

---

**Error — `dropOpenSearch` on a collection with no OpenSearch index:**
```json
{
  "errors": [{
    "message": "The collection 'articles' does not have an OpenSearch index to drop.",
    "errorCode": "OPEN_SEARCH_NOT_ENABLED_FOR_COLLECTION"
  }]
}
```

#### 4.3.5 Idempotency and `createCollection` convergence

> **The goal:** a client that runs `createCollection` with an `openSearch` block **and** a subsequent `alterCollection addOpenSearch` (or vice versa) must end up in a consistent, non-error state — just like re-running `createCollection` with `ifNotExists: true` works when the collection already exists.

There are two convergence scenarios to handle:

**Scenario A — `createCollection` after `alterCollection addOpenSearch`:**

1. The collection already exists with OpenSearch enabled (via `alterCollection`).
2. Client sends `createCollection` with the **same** `openSearch` block.
3. Expected: `{"status":{"ok":1}}` — idempotent success. The Data API compares the incoming `openSearch` block against the stored schema. If they match exactly (same `indexName`, `numShards`, `numReplicas`, `mappings`), the operation is a no-op.
4. If the `openSearch` block **differs** (e.g. different `mappings`), the Data API throws `EXISTING_COLLECTION_DIFFERENT_SETTINGS` — the same error thrown when any other collection option differs at re-creation time.

**Scenario B — `alterCollection addOpenSearch` after `createCollection` with `openSearch`:**

1. The collection already exists with OpenSearch enabled (created with `openSearch` block).
2. Client sends `alterCollection addOpenSearch`.
3. Expected: `OPEN_SEARCH_INDEX_ALREADY_EXISTS` — duplicate index guard. The resolver checks for an existing OpenSearch index before attempting to create one.

> **Why not silently skip duplicate `addOpenSearch`?** Making `addOpenSearch` silently idempotent would require comparing the new `mappings` against the existing stored schema and determining whether they are equivalent — a non-trivial operation because `mappings` can contain arbitrary OpenSearch JSON. The simpler, safer contract is: if an OpenSearch index already exists, reject with `OPEN_SEARCH_INDEX_ALREADY_EXISTS` (same as the tables path). The client should use `dropOpenSearch` first if a reconfiguration is needed.

**Settings comparison for `createCollection` idempotency:**

When `createCollection` is called and the collection already exists **with** an `openSearch` block, the Data API must compare the incoming `openSearch` descriptor against the stored `CollectionOpenSearchDef`. The comparison covers:

| Field | Match rule |
|---|---|
| `enabled` | Must both be `true` |
| `indexName` | Resolved values must match (incoming `null` → resolved to `hcd_<ks>_<coll>` before comparison) |
| `numShards` | Must match (both null, or both equal integers) |
| `numReplicas` | Must match (both null, or both equal integers) |
| `mappings` | JSON structural equality — `ObjectMapper.readTree(a).equals(ObjectMapper.readTree(b))` |

If all match → `{"status":{"ok":1}}` (idempotent success).
If any differ → `EXISTING_COLLECTION_DIFFERENT_SETTINGS`.
If the incoming `createCollection` has **no** `openSearch` block but the stored schema **has** one → `EXISTING_COLLECTION_DIFFERENT_SETTINGS` (OpenSearch is now part of the collection definition).

#### 4.3.6 Implementation Tips (server)

**New `AlterCollectionOperation` subtypes**

`alterCollection` already exists for tables (`AlterTableCommand` / `AlterTableOperation`). The collection equivalent follows the same pattern:

```
AlterCollectionCommand
  └── operation: AlterCollectionOperation  (sealed interface / @JsonTypeInfo)
        ├── AddOpenSearch(OpenSearchDesc openSearch)  (@JsonTypeName "addOpenSearch")
        └── DropOpenSearch()                          (@JsonTypeName "dropOpenSearch")
```

`AlterCollectionCommand` is a new command (`CommandName.ALTER_COLLECTION`, `CommandTarget.COLLECTION`) registered in `CollectionCommand`.

**`addOpenSearch` resolver logic**

1. Read `schemaObject.openSearchDef()`. If `openSearchDef.enabled()` is `true`, throw `OPEN_SEARCH_INDEX_ALREADY_EXISTS`.
2. Validate the incoming `openSearch` descriptor (same rules as `createCollection`): `enabled: true` required, `mappings` required, non-empty, no `_id` key.
3. Build the `WITH OPTIONS` map (same logic as `CreateCollectionOperation`): `unpackJsonFields`, `propertiesFromJsonFields`, `applyCustomSchema`, `applyDefaultSchema`, `customMappingsJson`, and optionally `indexName`, `numShards`, `numReplicas`.
4. Emit `CREATE CUSTOM INDEX IF NOT EXISTS hcd_<ks>_<coll> ON <ks>.<coll> (doc_json) USING 'OpenSearchIndex' WITH OPTIONS = {...}`.
5. Compute the new schema comment JSON: clone the existing `CollectionSettings`, set the `openSearch` block, bump `schemaVersion` to `V_3`, serialise and write back to the table comment.
6. Invalidate the schema cache for this collection.

**`dropOpenSearch` resolver logic**

1. Read `schemaObject.openSearchDef()`. If `!openSearchDef.enabled()`, throw `OPEN_SEARCH_NOT_ENABLED_FOR_COLLECTION`.
2. Resolve `saiIndexName = "hcd_" + keyspace + "_" + collectionName`.
3. Emit `DROP INDEX IF EXISTS <saiIndexName>`.
4. Compute the new schema comment JSON: clone existing `CollectionSettings`, clear the `openSearch` block (set to disabled / null), downgrade schema version if no other V_3 features are active, serialise and write back.
5. Invalidate the schema cache.

**`createCollection` idempotency check (update to existing resolver)**

In `CreateCollectionCommandResolver`, after detecting `EXISTING_COLLECTION` (collection already exists):
- If the incoming `options.openSearch` is non-null AND `existingSchema.openSearchDef().enabled()` is `true`, compare the two descriptors field by field (see §4.3.5 table). Mismatch → `EXISTING_COLLECTION_DIFFERENT_SETTINGS`.
- If the incoming `options.openSearch` is non-null AND `existingSchema.openSearchDef().enabled()` is `false`, throw `EXISTING_COLLECTION_DIFFERENT_SETTINGS`.
- If the incoming `options.openSearch` is null AND `existingSchema.openSearchDef().enabled()` is `true`, throw `EXISTING_COLLECTION_DIFFERENT_SETTINGS`.

**`findCollections` — updated response after `alterCollection`**

`findCollections` with `explain: true` reads the live schema from `CollectionSchemaObject.openSearchDef()`. Since `alterCollection` rewrites the table comment and invalidates the schema cache, the next `findCollections` call will reflect the updated state automatically — no additional changes required to the `findCollections` response path.

**New error codes required**

No new error codes are needed — all required codes (`OPEN_SEARCH_INDEX_ALREADY_EXISTS`, `OPEN_SEARCH_NOT_ENABLED_FOR_COLLECTION`, `OPEN_SEARCH_MISSING_FIELD_MAPPINGS`, `EXISTING_COLLECTION_DIFFERENT_SETTINGS`) are already defined.

**Todo list**
1. Create `AlterCollectionCommand` record (`CommandName.ALTER_COLLECTION`, `CommandTarget.COLLECTION`) in `api/model/command/impl`, registered in `CollectionCommand` alongside `AlterTableCommand`.
2. Create `AlterCollectionOperation` sealed interface with `@JsonTypeInfo`, and `AddOpenSearch` / `DropOpenSearch` inner records in `api/model/command/impl`.
3. Create `AlterCollectionCommandResolver` in `service/resolver` — implements `CommandResolver<AlterCollectionCommand>`, `@ApplicationScoped`.
4. `addOpenSearch` path: validate, build `WITH OPTIONS`, emit `CREATE CUSTOM INDEX`, update table comment, invalidate schema cache.
5. `dropOpenSearch` path: validate, resolve SAI name, emit `DROP INDEX`, update table comment, invalidate schema cache.
6. Update `CreateCollectionCommandResolver` — in the `EXISTING_COLLECTION` branch, extend the settings comparison to include the `openSearch` block (see §4.3.5).
7. Unit tests: `addOpenSearch` on clean collection; `addOpenSearch` on already-enabled collection → error; `dropOpenSearch` on enabled → success; `dropOpenSearch` on disabled → error; `createCollection` idempotency with matching and mismatched `openSearch` blocks.

**Relevant context**
- Pattern command: `AlterTableCommand` / `AlterTableOperationImpl`
- Pattern resolver: `AlterTableCommandResolver`
- Collection creation with OpenSearch: `CreateCollectionCommandResolver`, `CreateCollectionOperation`
- Schema settings: `CollectionOpenSearchDef`, `CollectionSettingsV3Reader`

#### 4.3.7 Implementation Tips (clients)

**Java**
```java
// Add OpenSearch to an existing collection
database.getCollection("articles").alter(
    AlterCollectionOperation.addOpenSearch(
        OpenSearchOptions.builder()
            .indexName("articles-full-text")
            .mappings(Map.of(
                "title",  Map.of("type", "text", "analyzer", "english"),
                "body",   Map.of("type", "text", "analyzer", "english"),
                "author", Map.of("type", "keyword")
            ))
            .build()
    )
);

// Drop OpenSearch from an existing collection
database.getCollection("articles").alter(
    AlterCollectionOperation.dropOpenSearch()
);
```

**Python**
```python
# Add OpenSearch to an existing collection
collection.alter(add_open_search=OpenSearchOptions(
    index_name="articles-full-text",
    mappings={
        "title":  {"type": "text", "analyzer": "english"},
        "body":   {"type": "text", "analyzer": "english"},
        "author": {"type": "keyword"},
    }
))

# Drop OpenSearch from an existing collection
collection.alter(drop_open_search={})
```

**TypeScript / Node.js**
```typescript
// Add OpenSearch to an existing collection
await collection.alter({
  addOpenSearch: {
    enabled: true,
    indexName: "articles-full-text",
    mappings: {
      title:  { type: "text", analyzer: "english" },
      body:   { type: "text", analyzer: "english" },
      author: { type: "keyword" }
    }
  }
});

// Drop OpenSearch from an existing collection
await collection.alter({ dropOpenSearch: {} });
```

---

## 5. Impacted Commands — Tables

---

### 5.1 `createOpenSearchIndex` — new DDL command for Tables

#### 5.1.1 Goal

A new dedicated `createOpenSearchIndex` command is added for tables, following the same pattern as `createTextIndex` and `createVectorIndex` — one command per index type. Note: `createIndex` is a **sibling** command (for non-vector, non-analyzer SAI indexes), not a superclass or "generic" form of the others. All four commands sit at the same level.

> **SAI index name vs. OS index name for tables.** The two names are both user-configurable here, which makes the distinction especially important:
> - The **SAI index name** is the `name` field at the top level of the command (e.g. `"name": "articles_search"`). This becomes the Cassandra custom index identifier (`CREATE CUSTOM INDEX IF NOT EXISTS articles_search`) and is what is referenced in every subsequent `expr(articles_search, ...)` query. Users must choose this name and cannot change it after creation.
> - The **OS index name** is `definition.options.openSearchIndexName` (e.g. `"openSearchIndexName": "articles-full-text"`). This is the OpenSearch index name passed as `'indexName'` in `WITH OPTIONS`. It is optional — when omitted it defaults to `hcd_<keyspace>_<tableName>`. The field is named `openSearchIndexName` (not `indexName`) to avoid ambiguity with the SAI index name above.

Users also optionally list which columns to replicate (omitting the list replicates all non-primary-key columns) and can provide a `mappings` JSON object to override field types. Only one OpenSearch custom index per table is permitted by HCD.

After the index is created, the table's `find` command accepts the `$openSearch` operator (see §4.5).

#### 5.1.2 How it works

```mermaid
sequenceDiagram
    participant APP as Application
    participant API as Data API
    participant CQL as Cassandra

    APP->>API: createOpenSearchIndex<br/>name: articles_search<br/>columns: [title,body,author]<br/>options.openSearchIndexName: articles-ft
    API->>API: validate columns exist on table<br/>check no existing OpenSearch index
    API->>CQL: CREATE CUSTOM INDEX articles_search<br/>ON ks.articles (title, body, author)<br/>USING OpenSearchIndex<br/>WITH OPTIONS indexName:articles-ft
    CQL-->>API: OK
    API-->>APP: {"status":{"ok":1}}
```

#### 5.1.3 OpenSearch fields in the Data API Payload

> **Cassandra column type inference for Tables (`applyDefaultSchema: true`):**
> Unlike Collections (which store raw JSON in an un-typed `doc_json TEXT` column and therefore **require** explicit `mappings`), Tables have a strongly-typed Cassandra CQL schema. When `mappings` is omitted, HCD automatically inspects the table's column definitions and maps CQL types to OpenSearch field types:
>
> | Cassandra CQL Column Type | Inferred OpenSearch Type | Default Indexing Behavior |
> |---|---|---|
> | `text`, `varchar`, `ascii` | `text` (with multi-field `keyword`) | Full-text tokenized + exact-match subfield |
> | `int`, `smallint`, `tinyint` | `integer` | 32-bit whole number |
> | `bigint`, `varint`, `counter` | `long` | 64-bit whole number |
> | `float` | `float` | 32-bit floating point |
> | `double`, `decimal` | `double` | 64-bit floating point |
> | `boolean` | `boolean` | `true` / `false` |
> | `timestamp`, `date` | `date` | Epoch millis / ISO-8601 timestamps |
> | `time` | `keyword` | Time string (e.g. `"13:30:00"`) |
> | `uuid`, `timeuuid` | `keyword` | Canonical UUID string |
> | `blob` | `binary` | Base64-encoded binary data |
> | `inet` | `ip` | IPv4 / IPv6 address |
> | `vector<float, N>` | `knn_vector` (`dimension: N`) | Dense vector for k-NN search |
> | `list<T>`, `set<T>` | Array of `<T>` | Multi-valued element field |
> | `map<text, T>`, `UDT` | `object` | Structured sub-fields |

> **When to supply custom `definition.options.mappings` for Tables:**
> The `definition.options.mappings` field is **optional** on tables. Provide it only when you want to override the default inferred mapping for specific columns, such as:
> - **Language-specific analyzers:** e.g. `"title": { "type": "text", "analyzer": "english" }`
> - **Exact-match only strings:** forcing a string column to `{"type": "keyword"}` without text tokenization
> - **Specific date format patterns:** `"published_at": { "type": "date", "format": "strict_date_optional_time||epoch_millis" }`
> - **Advanced k-NN vector indexing:** `"embedding": { "type": "knn_vector", "dimension": 1536, "method": { "name": "hnsw", "engine": "nmslib" } }`
> - **UDT / nested object sub-fields:** `"metadata": { "properties": { "source": { "type": "keyword" } } }`
>
> When `mappings` is supplied for a table, the Data API emits `applyCustomSchema: 'true'` + `applyDefaultSchema: 'false'` and serialises `mappings` wrapped under `{"properties":{...}}` as `customMappingsJson`. No `_id` is injected for tables (tables use primary key columns).

| Field | Required | Default | Description |
|---|---|---|---|
| `name` | **yes** | — | **SAI index name** — the Cassandra custom index identifier. This name is used in `CREATE CUSTOM INDEX IF NOT EXISTS <name>` and in every subsequent `expr(<name>, ...)` query. Must be unique within the keyspace. Cannot be changed after creation. |
| `definition.columns` | no | *(all non-PK columns)* | List of table columns to replicate. Omit to replicate all non-primary-key columns. |
| `definition.options.numShards` | no | *(HCD default)* | Number of primary shards for the OS index. When omitted, HCD uses its own default. |
| `definition.options.numReplicas` | no | *(HCD default)* | Number of replicas for the OS index (analogous to Cassandra replication factor). When omitted, HCD uses its own default. |
| `definition.options.openSearchIndexName` | no | `hcd_<keyspace>_<tableName>` | **OS index name** — the OpenSearch index name (passed as `'indexName'` in `WITH OPTIONS`). Independent of the SAI index name above. Auto-generated when absent. Renamed from `indexName` to avoid ambiguity with the SAI index name (see §1 terminology box). |
| `definition.options.mappings` | no | *(HCD infers from Cassandra schema)* | Flat field-name → OpenSearch field definition map as a **real JSON object** (not a string). For tables, HCD derives the mapping automatically from the Cassandra column types (`applyDefaultSchema: 'true'`). Provide only when you need custom analyzers or types the default schema would not produce. The Data API wraps it under `{"properties":{...}}` and serialises the result to a JSON string for the CQL `WITH OPTIONS` `customMappingsJson` key. |
| `options.ifNotExists` | no | `false` | Do not error if a Cassandra custom index with this SAI index name already exists. |

#### 5.1.4 Sample payloads

**Minimal — index all non-PK columns with HCD default mapping (no shard/partition count needed):**
```json
{
  "createOpenSearchIndex": {
    "name": "users_search"
  }
}
```
→ CQL emitted:
```cql
-- SAI index name: users_search          (Cassandra identifier, used in expr())
-- OS index name:  hcd_mykeyspace_users  (OpenSearch index — defaults to hcd_<ks>_<table> when 'indexName' is absent)
CREATE CUSTOM INDEX IF NOT EXISTS users_search
ON mykeyspace.users ()
USING 'OpenSearchIndex'
WITH OPTIONS = {
  'createIndexIfNotExists': 'true'
};
```

---

**Specific columns, default HCD mapping derived from schema:**
```json
{
  "createOpenSearchIndex": {
    "name": "articles_search",
    "definition": {
      "columns": ["title", "body", "tags", "author"]
    },
    "options": {
      "ifNotExists": true
    }
  }
}
```
→ CQL emitted:
```cql
-- SAI index name: articles_search            (Cassandra identifier, used in expr())
-- OS index name:  hcd_mykeyspace_articles    (OpenSearch index — defaulted)
CREATE CUSTOM INDEX IF NOT EXISTS articles_search
ON mykeyspace.articles (title, body, tags, author)
USING 'OpenSearchIndex'
WITH OPTIONS = {
  'createIndexIfNotExists': 'true'
};
```

---

**Specific columns with custom OpenSearch field mappings:**
```json
{
  "createOpenSearchIndex": {
    "name": "articles_search",
    "definition": {
      "columns": ["title", "body", "author", "published_at"],
      "options": {
        "openSearchIndexName": "articles-full-text",
        "mappings": {
          "title":        { "type": "text", "analyzer": "english" },
          "body":         { "type": "text", "analyzer": "english" },
          "author":       { "type": "keyword" },
          "published_at": { "type": "date", "format": "strict_date_optional_time" }
        }
      }
    },
    "options": {
      "ifNotExists": true
    }
  }
}
```
→ CQL emitted (the Data API wraps `mappings` under `{"properties":{...}}` and serialises to a JSON string before embedding it in `WITH OPTIONS`):
```cql
-- SAI index name: articles_search   (Cassandra identifier, used in expr())
-- OS index name:  articles-full-text (OpenSearch index, set explicitly via definition.options.openSearchIndexName)
CREATE CUSTOM INDEX IF NOT EXISTS articles_search
ON mykeyspace.articles (title, body, author, published_at)
USING 'OpenSearchIndex'
WITH OPTIONS = {
  'createIndexIfNotExists': 'true',
  'indexName':              'articles-full-text',
  'applyCustomSchema':      'true',
  'applyDefaultSchema':     'false',
  'customMappingsJson':     '{"properties":{"title":{"type":"text","analyzer":"english"},"body":{"type":"text","analyzer":"english"},"author":{"type":"keyword"},"published_at":{"type":"date","format":"strict_date_optional_time"}}}'
};
```

---

**Using `applyDefaultSchema: false` to rely on an OpenSearch index template:**
```json
{
  "createOpenSearchIndex": {
    "name": "events_search",
    "definition": {
      "columns": ["event_type", "payload", "region"],
      "options": {
        "openSearchIndexName": "events-search",
        "applyDefaultSchema": false
      }
    }
  }
}
```

---

**`listIndexes` response showing an OpenSearch index alongside SAI indexes:**
```json
{
  "listIndexes": {}
}
```
Response:
```json
{
  "status": {
    "indexes": [
      {
        "name": "articles_title_sai",
        "indexType": "regular",
        "definition": {
          "column": "title"
        }
      },
      {
        "name": "articles_search",
        "indexType": "openSearch",
        "definition": {
          "columns": ["title", "body", "author", "published_at"],
          "options": {
            "openSearchIndexName": "articles-full-text",
            "applyDefaultSchema": false
          }
        }
      }
    ]
  }
}
```

> In this response: `"name": "articles_search"` is the **SAI index name** (Cassandra custom index identifier, used in `expr(articles_search, ...)`). `"definition.options.openSearchIndexName": "articles-full-text"` is the **OS index name** (the OpenSearch index). These two names are independent and serve different purposes.

#### 5.1.5 Implementation Tips (server)

**Behaviour rules**
- `createOpenSearchIndex` is a dedicated command registered alongside `createTextIndex` and `createVectorIndex` — not an extension of the generic `createIndex` command.
- The `definition.columns` list is optional. When absent, the CQL `ON table ()` form is emitted (empty column list) and HCD replicates all non-primary-key columns.
- After creation, `listIndexes` returns the new index with `indexType: "openSearch"`, serialised via `ApiOpenSearchIndex.getSchemaDescription()`. The entry exposes both the **SAI index name** (from `name`) and the **OS index name** (from `definition.options.openSearchIndexName`, resolved).
- Only one OpenSearch custom index (SAI index name) is permitted per table. The resolver must call `schemaObject.apiTableDef().indexes()` and reject with `OPEN_SEARCH_INDEX_ALREADY_EXISTS` if any index of type `OPEN_SEARCH` already exists.
- `options.ifNotExists` maps to `IF NOT EXISTS` in the CQL `CREATE CUSTOM INDEX` statement (guards the SAI index name), consistent with `createTextIndex` behaviour.

**CQL emitted**

`CreateIndexDBTask.buildStatement()` currently hard-codes `CQLSAIIndex.SAI_CLASS_NAME` (`StorageAttachedIndex`) as the custom index class. For OpenSearch indexes the class must be `OpenSearchIndex` instead. The fix is to read the class name from `ApiIndexDef` rather than hard-coding it. `ApiOpenSearchIndex` will expose `OPEN_SEARCH_CLASS_NAME = "OpenSearchIndex"` and override `indexClassName()` accordingly.

**`WITH OPTIONS` mapping rules** (for `createOpenSearchIndex`)

> The SAI index name appears in the `CREATE CUSTOM INDEX IF NOT EXISTS <sai-index-name>` statement itself, not inside `WITH OPTIONS`. The OS index name is the only `WITH OPTIONS` key that controls the OpenSearch-side index identity.

| Condition | Option key added |
|---|---|
| Always | `createIndexIfNotExists: 'true'` |
| `definition.options.numShards` set by user | `numShards: '<value>'` |
| `definition.options.numReplicas` set by user | `numReplicas: '<value>'` |
| `definition.options.openSearchIndexName` set by user (OS index name) | `indexName: '<value>'` |
| `definition.options.mappings` set | `applyCustomSchema: 'true'`, `applyDefaultSchema: 'false'`, `customMappingsJson: '<serialised-json>'` (the Data API wraps `mappings` under `{"properties":{...}}` and serialises to a JSON string) |
| `definition.options.applyDefaultSchema: false` | `applyDefaultSchema: 'false'` |

> Unlike the collection path, `unpackJsonFields` is **not** set here — tables have typed columns so HCD does not need to unpack a JSON blob.

**Todo list**
1. Create `CreateOpenSearchIndexCommand` record in `api/model/command/impl` mirroring `CreateTextIndexCommand` — fields: `String name`, `OpenSearchIndexDefinitionDesc definition`, `String indexType`, `CommandOptions options`.
2. Create `OpenSearchIndexDefinitionDesc` record in `api/model/command/table/definition/indexes` — fields: `List<String> columns`, `OpenSearchIndexOptions options` (inner record: `numShards`, `numReplicas`, `openSearchIndexName`, `JsonNode mappings`, `applyDefaultSchema`).
3. Add `OPEN_SEARCH("openSearch")` to [`ApiIndexType`](src/main/java/io/stargate/sgv2/jsonapi/service/schema/tables/ApiIndexType.java) enum, and add `OPEN_SEARCH_CLASS_NAME = "OpenSearchIndex"` constant.
4. Update [`IndexFactoryFromCql.isSupported()`](src/main/java/io/stargate/sgv2/jsonapi/service/schema/tables/factories/IndexFactoryFromCql.java) to return `true` for both SAI indexes and OpenSearch indexes: `CQLSAIIndex.isSAIIndex(indexMetadata) || isOpenSearchIndex(indexMetadata)` where `isOpenSearchIndex` checks `class_name == "OpenSearchIndex"`.
5. Create `ApiOpenSearchIndex` class in `service/schema/tables` following `ApiTextIndex` — includes `FROM_DESC_FACTORY` (`UserDescFactory` creates from `OpenSearchIndexDefinitionDesc`) and `FROM_CQL_FACTORY` (creates from `IndexMetadata` when `class_name` is `OpenSearchIndex`).
6. Update [`IndexFactoryFromCql.create()`](src/main/java/io/stargate/sgv2/jsonapi/service/schema/tables/factories/IndexFactoryFromCql.java) switch to handle the `OPEN_SEARCH` case by delegating to `ApiOpenSearchIndex.FROM_CQL_FACTORY`.
7. Update [`CreateIndexDBTask.buildStatement()`](src/main/java/io/stargate/sgv2/jsonapi/service/operation/tables/CreateIndexDBTask.java) to read the custom index class name from `ApiIndexDef` rather than hard-coding `SAI_CLASS_NAME` — add `indexClassName()` method to `ApiIndexDef` base class with default `SAI_CLASS_NAME`, overridden to `OPEN_SEARCH_CLASS_NAME` in `ApiOpenSearchIndex`.
8. Create `CreateOpenSearchIndexCommandResolver` in `service/resolver` following `CreateTextIndexCommandResolver` — validates no duplicate OpenSearch index; resolves the OS index name default (`hcd_<keyspace>_<tableName>` when `definition.options.openSearchIndexName` is absent); when `mappings` is provided, wraps it under `{"properties":{...}}` and serialises to a JSON string for `customMappingsJson`; builds `ApiOpenSearchIndex` via `FROM_DESC_FACTORY`; hands off to `CreateIndexDBTaskBuilder`. The SAI index name is taken directly from `command.name`.
9. Register the command in the command router alongside other index creation commands.
10. Update `listIndexes` serialisation to include `openSearch` index type in the index type enum accepted by the list response serialiser.

**Relevant context**
- Pattern command: [`CreateTextIndexCommand`](src/main/java/io/stargate/sgv2/jsonapi/api/model/command/impl/CreateTextIndexCommand.java)
- Pattern resolver: [`CreateTextIndexCommandResolver`](src/main/java/io/stargate/sgv2/jsonapi/service/resolver/CreateTextIndexCommandResolver.java)
- Pattern model: [`ApiTextIndex`](src/main/java/io/stargate/sgv2/jsonapi/service/schema/tables/ApiTextIndex.java)
- Index task: [`CreateIndexDBTask`](src/main/java/io/stargate/sgv2/jsonapi/service/operation/tables/CreateIndexDBTask.java) — `buildStatement()` to be updated
- Type enum: [`ApiIndexType`](src/main/java/io/stargate/sgv2/jsonapi/service/schema/tables/ApiIndexType.java) — add `OPEN_SEARCH`
- Factory: [`IndexFactoryFromCql`](src/main/java/io/stargate/sgv2/jsonapi/service/schema/tables/factories/IndexFactoryFromCql.java) — `isSupported()` and `create()` both require changes

#### 5.1.6 Implementation Tips (clients)

> Client SDK documentation: [HCD API Reference — Instantiate Client](https://docs.datastax.com/en/hyper-converged-database/2.0/api-reference/instantiate-client.html) and sub-pages.

**Java**
```java
database.getTable("articles").createOpenSearchIndex(
    "articles_search",
    OpenSearchIndexDefinition.builder()
        .columns("title", "body", "author", "published_at")
        .openSearchIndexName("articles-full-text")
        .mappings(
            OpenSearchMappings.builder()
                .field("title", "text", "english")
                .build()
        )
        .build(),
    CreateOpenSearchIndexOptions.builder()
        .ifNotExists(true)
        .build()
);
```

**Python**

> Use named classes for `OpenSearchIndexDefinition` rather than raw dictionaries when available. The `mappings` field is a real JSON-compatible object (not a serialised string).

```python
from astrapy.info import OpenSearchIndexDefinition, OpenSearchIndexOptions

table = database.get_table("articles")
table.create_open_search_index(
    "articles_search",
    definition=OpenSearchIndexDefinition(
        columns=["title", "body", "author", "published_at"],
        options=OpenSearchIndexOptions(
            open_search_index_name="articles-full-text",
            mappings={
                "title":  {"type": "text", "analyzer": "english"},
            },
        ),
    ),
    if_not_exists=True,
)
```

**TypeScript / Node.js**
```typescript
const table = db.table("articles");
await table.createOpenSearchIndex("articles_search", {
  definition: {
    columns: ["title", "body", "author", "published_at"],
    options: {
      openSearchIndexName: "articles-full-text",
      mappings: {
        title: { type: "text", analyzer: "english" }
      }
    }
  },
  options: { ifNotExists: true }
});
```

---

### 4.4 `find` on Collections — new `$openSearch` filter operator

#### 4.4.1 Goal

Collections created with `openSearch.enabled: true` gain a new `$openSearch` filter operator. The value is an OpenSearch query DSL object — the full expressive power of OpenSearch queries (match, bool, range, term, multi_match, etc.) is available. The Data API routes the query to Cassandra's `expr(...)` predicate which delegates to the HCD OpenSearch query handler; no change is required on the client beyond using the new filter key.

> **Scope:** While this section focuses on `find`, the `$openSearch` filter operator applies to **all commands that accept a filter** — including `findOne`, `findOneAndUpdate`, `findOneAndReplace`, `findOneAndDelete`, `updateOne`, `updateMany`, `deleteOne`, `deleteMany`, and `countDocuments`. The same rules apply everywhere: `$openSearch` must be the sole filter key, and the collection must have been created with `openSearch.enabled: true`.

`$openSearch` must be the only key in the filter — it cannot be combined with other operators in the same request. `projection` works normally.

**Default result order is OpenSearch relevance.** When no `sort` clause is provided, documents are returned in the order OpenSearch produces them — descending by relevance score. This is the natural and expected behaviour of a `$openSearch` query. Adding a `sort` clause overrides this ordering with an in-memory sort on document fields (see §4.4.2), which discards the relevance ranking.

**Paging — Cassandra `pageState` only.** The only supported paging mechanism for `$openSearch` is the standard Data API `pageState` token. DSL-level pagination (`from`/`size` and `search_after`) is **not supported** — the Data API does not pass `size`, `from`, or `search_after` fields through to the `expr(...)` predicate.

> **How it works:** Set `options.limit` to control the page size. HCD re-runs the OpenSearch query on each CQL page fetch and returns the next slice of matching rows via Cassandra's native cursor. The response includes `nextPageState` when more results exist; pass it back in `options.pageState` on the next request.

> **Constraint:** capped at 10 000 total results (OpenSearch `max_result_window`). Results may shift between pages if documents are written concurrently.

#### 4.4.2 How it works

`$openSearch` is one of four search-path operators a collection can expose. They are **mutually exclusive per query** — each one takes over the full CQL routing for that request and cannot share a `filter` object with the others or with standard field predicates.

| Operator | Requires at creation | Routed via | Can combine with `sort`? | Can combine with rerank? |
|---|---|---|---|---|
| `$match` | `lexical.enabled: true` | SAI text index | Yes (CQL `ORDER BY`) | Yes — rerank re-scores `$match` results |
| `$vector` | `vector` block | ANN index | No | Yes — rerank re-scores ANN results |
| `$hybrid` | `vector` + `lexical` | ANN + SAI fused | No | Yes — rerank re-scores fused results |
| `$openSearch` | `openSearch.enabled: true` | `expr(...)` → OpenSearch | Yes — two kinds (see below) | No — not supported in this version |

**Rerank and `$openSearch`:** rerank is currently not supported in combination with `$openSearch`. The rerank pipeline expects a candidate set produced by the Data API's internal ANN or lexical paths; the `expr(...)` result set is owned by OpenSearch and returned as already-ranked rows by relevance score. A future version could pipe `$openSearch` results through rerank, but that is out of scope here.

**`$openSearch` + `sort` — two distinct meanings:** There are two ways to sort with `$openSearch`, and they are very different:
1. **Sort inside the `$openSearch` DSL** (e.g. `$openSearch: { "sort": [...], "query": {...} }`) — this sort is passed verbatim to OpenSearch and executed server-side. OpenSearch applies it before returning hits. This is the recommended way to order results when using `$openSearch`.
2. **Data API `sort` clause alongside `$openSearch`** (e.g. `find: { filter: {$openSearch: ...}, sort: {field: 1} }`) — this is a **post-fetch, in-memory** sort applied by the Data API after OpenSearch has already returned results. It does **not** influence the OpenSearch query, and it discards the OpenSearch relevance ranking. Use this only when you need to sort on a field that is not in the OpenSearch index.

**Sequence diagram — request flow:**

```mermaid
sequenceDiagram
    participant APP as Application
    participant API as Data API
    participant CQL as CQL / Cassandra
    participant OQH as OpenSearchQueryHandler
    participant OS as OpenSearch

    APP->>API: find\nfilter: {$openSearch: {match: {name: Widget}}}
    API->>API: FilterResolver recognises $openSearch key\nreads openSearchDef.saiIndexName (SAI index name)\nvalidates openSearch.enabled true\nbuilds OpenSearchCollectionFilter
    API->>CQL: SELECT key, doc_json FROM products\nWHERE expr(hcd_ks_products,\n{"match":{"name":"Widget"}})\n(hcd_ks_products = SAI index name)
    CQL->>OQH: intercept expr() predicate\nresolves SAI index → OS index name
    OQH->>OS: POST /<os-index-name>/_search\n{"query":{"match":{"name":"Widget"}}}
    OS-->>OQH: hits: [{_id:...}, ...]
    OQH-->>CQL: Cassandra rows matching PKs
    CQL-->>API: rows with doc_json
    API->>API: deserialise doc_json\napply projection
    API-->>APP: {data: {documents: [{...}]}}
```

#### 4.4.3 OpenSearch fields in the Data API Payload

The `$openSearch` filter value is an **OpenSearch Query DSL** object. It is serialised to JSON and passed into the CQL `expr(<saiIndexName>, '<dsl>')` predicate. The Data API passes the DSL through with **one exception**: any `_id` value inside a `term`, `terms`, or `ids` query clause is rewritten from the Data API string form to the HCD composite-key encoding before the `expr(...)` is assembled (see the `_id` rewriting callout below).

| Filter field | Type | Description |
|---|---|---|
| `$openSearch` | Object | Root key — must be the **only** key in the `filter` object. Its value is any valid OpenSearch query clause. |
| `$openSearch.<queryType>` | Object | Any OpenSearch leaf or compound query: `match`, `multi_match`, `match_phrase`, `bool`, `term`, `range`, `wildcard`, `fuzzy`, `match_all`, etc. |

> **Same field in `$openSearch` and an outer filter?** This is structurally impossible — `$openSearch` must be the only key in the filter object, so there is no outer filter to conflict with. If you need to filter on a field that also appears in an OpenSearch query, put both conditions inside the `bool` query within the `$openSearch` value (e.g. `"bool": {"must": [...], "filter": [...]}`).

> **`_id` rewriting — `term`, `terms`, and `ids` queries.** The Data API `_id` field (e.g. `"product-3"`) is intentionally excluded from the OpenSearch `_source` (it is not in `propertiesFromJsonFields`) because OpenSearch permanently rejects `_id` as a body field. Instead, OpenSearch's reserved metadata `_id` carries the **Cassandra composite primary-key encoding** that HCD sets at index time: `{_0=<partitionKey>, _1=<clusteringKey>}`. For a collection with a single-value `_id` of `"product-3"`, the Cassandra PK is `(partition=1, clustering=product-3)`, so the OS `_id` is the string `{_0=1, _1=product-3}`. A plain `{"term": {"_id": "product-3"}}` query therefore **never matches** — the user-visible `_id` and the OS metadata `_id` are different strings.
>
> To bridge this gap, the Data API **rewrites `_id` values** in the DSL before serialising the `expr(...)` predicate:
> - **`term` on `_id`:** `{"term": {"_id": "product-3"}}` → `{"term": {"_id": "{_0=1, _1=product-3}"}}`
> - **`terms` on `_id`:** `{"terms": {"_id": ["product-2", "product-3"]}}` → `{"terms": {"_id": ["{_0=1, _1=product-2}", "{_0=1, _1=product-3}"]}}`
> - **`ids` query:** `{"ids": {"values": ["product-2", "product-3"]}}` → `{"ids": {"values": ["{_0=1, _1=product-2}", "{_0=1, _1=product-3}"]}}`
>
> The rewriting applies recursively when these clauses appear inside a `bool` query (`must`, `should`, `must_not`, `filter` arrays). All other query types (`match`, `range`, `wildcard`, etc.) are passed through unchanged.
>
> **`search_after` cursor with `_id` tiebreaker:** when a client sorts by `_id` inside the DSL and uses `search_after` for deep paging, the cursor value returned by OpenSearch is already in the HCD encoding (`{_0=1, _1=product-3}`). The client must pass this opaque string back verbatim as the `search_after` value — no rewriting is needed or applied.

**Supported OpenSearch query types (non-exhaustive)**

| Query type | Use case |
|---|---|
| `match` | Full-text search on a single field with analysis |
| `multi_match` | Full-text search across multiple fields |
| `match_phrase` | Exact phrase match |
| `bool` | Compound queries with `must`, `should`, `must_not`, `filter` clauses |
| `term` | Exact value match (no analysis, for keyword fields) |
| `range` | Numeric or date range filtering |
| `wildcard` | Prefix / glob pattern matching |
| `fuzzy` | Approximate match with edit distance |
| `match_all` | Returns all indexed documents |

#### 4.4.4 Sample payloads

**Simple term match — find all products whose `name` contains "Widget":**
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "match": { "name": "Widget" }
      }
    }
  }
}
```

---

**Paging — strategy 1: Cassandra `pageState` (simplest, relevance order, ≤10 000 total results)**

Page 1 — no `pageState`:
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "match": { "description": "high performance cassandra database" }
      }
    },
    "options": { "limit": 20 }
  }
}
```
Response carries `"nextPageState": "<token>"`. Pass it back for page 2:
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "match": { "description": "high performance cassandra database" }
      }
    },
    "options": { "limit": 20, "pageState": "<token from page 1>" }
  }
}
```
> Results are returned in OpenSearch relevance order. HCD re-runs the OpenSearch query on each CQL page request, so results may shift if documents are written between pages. Capped at 10 000 total hits.

---

**Paging — strategy 2: `from` / `size` inside the DSL (offset paging, up to 10 000)**

Page 1 — `from: 0, size: 20`:
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "from": 0,
        "size": 20,
        "query": {
          "match": { "description": "high performance cassandra database" }
        }
      }
    }
  }
}
```
Page 2 — `from: 20, size: 20`:
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "from": 20,
        "size": 20,
        "query": {
          "match": { "description": "high performance cassandra database" }
        }
      }
    }
  }
}
```
> The `from`/`size` window is resolved entirely inside OpenSearch before the PKs reach Cassandra. No Data API `pageState` is needed — the client tracks the offset. OpenSearch limits this to 10 000 total results; use `search_after` for deeper paging.

---

**Paging — strategy 3: `search_after` (deep paging, unlimited, requires a sort tiebreaker)**

Page 1 — initial query with a deterministic `sort`:
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "size": 20,
        "query": {
          "match": { "description": "cassandra database" }
        },
        "sort": [
          { "_score": { "order": "desc" } },
          { "_id":    { "order": "asc"  } }
        ]
      }
    }
  }
}
```
The last document in the response will have sort values like `[0.87, "{_0=1, _1=doc-abc123}"]`. The `_id` tiebreaker is the HCD composite-key encoding — pass it back verbatim as the `search_after` value, no rewriting required:
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "size": 20,
        "query": {
          "match": { "description": "cassandra database" }
        },
        "sort": [
          { "_score": { "order": "desc" } },
          { "_id":    { "order": "asc"  } }
        ],
        "search_after": [0.87, "{_0=1, _1=doc-abc123}"]
      }
    }
  }
}
```
> `search_after` is a live cursor — it works on the current state of the index, so new documents written between requests can appear on subsequent pages. Adding `_id` as the final tiebreaker ensures no duplicates even if scores are equal. No 10 000 cap applies. The sort lives **inside** the OpenSearch DSL; it is not the Data API `sort` clause and does not trigger in-memory sorting. The `_id` cursor value (`{_0=1, _1=doc-abc123}`) is in the HCD composite-key encoding returned by OpenSearch — pass it back verbatim; the Data API does not rewrite `search_after` values.

---

**Multi-field match — search across multiple document fields:**
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "multi_match": {
          "query": "machine learning tutorial",
          "fields": ["title", "body", "tags"]
        }
      }
    }
  }
}
```

---

**Boolean query — must match + range filter:**
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "bool": {
          "must": [
            { "match": { "description": "cassandra database" } }
          ],
          "filter": [
            { "range": { "price": { "gte": 5.0, "lte": 100.0 } } },
            { "term": { "category": "database" } }
          ]
        }
      }
    },
    "options": { "limit": 10 }
  }
}
```

---

**Range query on an EJSON Date field (`publishedAt.$date`):**
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "range": {
          "publishedAt.$date": {
            "gte": 1730419200000,
            "lte": 1735689600000
          }
        }
      }
    }
  }
}
```

> For collections storing dates via the EJSON `{"$date": 123...}` format, the range query targets the nested field path `publishedAt.$date` (see §3.1.1).

---

**Match all — retrieve all indexed documents (useful for testing/debugging):**
```json
{
  "find": {
    "filter": {
      "$openSearch": { "match_all": {} }
    }
  }
}
```

---

**Phrase match — exact phrase in a field:**
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "match_phrase": { "body": "distributed database cluster" }
      }
    },
    "projection": { "title": 1, "author": 1, "publishedAt": 1 }
  }
}
```

---

**Wildcard / prefix search:**
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "wildcard": { "title": "data*" }
      }
    }
  }
}
```

---

**Combined with `findOne`:**
```json
{
  "findOne": {
    "filter": {
      "$openSearch": {
        "term": { "author": "Alice Smith" }
      }
    }
  }
}
```

---

**Combined with `findOneAndUpdate` — update the first matching OpenSearch result:**
```json
{
  "findOneAndUpdate": {
    "filter": {
      "$openSearch": {
        "match": { "title": "Getting Started" }
      }
    },
    "update": {
      "$set": { "status": "published" }
    }
  }
}
```

---

**k-NN vector search via OpenSearch — pass-through for advanced users who configure a `knn_vector` mapping:**
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "knn": {
          "embedding": {
            "vector": [0.12, 0.85, 0.34, 0.67],
            "k": 10
          }
        }
      }
    }
  }
}
```
> **Prerequisite:** the collection's OpenSearch index must declare the `embedding` field with `"type": "knn_vector"` in its `mappings`. Provide this at collection creation via `openSearch.mappings`:
> ```json
> "mappings": { "embedding": { "type": "knn_vector", "dimension": 4 } }
> ```
> This is an advanced pattern for users who want OpenSearch-backed **KNN** (k-nearest-neighbour) search. Note the difference from the Data API's native `$vector` operator: `$vector` uses SAI's **ANN** (approximate nearest-neighbour) index which is orders of magnitude faster than exact KNN. OpenSearch `knn_vector` performs **exact KNN** — results are more precise but at a significant performance cost at scale. For most vector search use cases, the Data API's native `$vector` operator is the recommended path.

---

**`neural` sparse vector query — for OpenSearch neural-sparse models:**
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "neural_sparse": {
          "passage_embedding": {
            "query_text": "distributed cassandra database",
            "model_id": "aB3dEfGh"
          }
        }
      }
    }
  }
}
```
> **Prerequisite:** the OpenSearch index must have a `rank_features` (sparse) field declared in `mappings` and the neural-sparse model must be deployed in the OpenSearch cluster. Pass the model ID at query time. Like `knn`, this is a pass-through advanced pattern with no Data API validation of the DSL payload.

---

**`neural` dense vector query — model-backed dense embedding lookup:**
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "neural": {
          "passage_embedding": {
            "query_text": "real-time analytics with Cassandra",
            "model_id": "aB3dEfGh",
            "k": 5
          }
        }
      }
    }
  }
}
```

---

**Error case — `$openSearch` combined with another filter operator:**
```json
{
  "find": {
    "filter": {
      "$openSearch": { "match": { "title": "tutorial" } },
      "status": { "$eq": "published" }
    }
  }
}
```
Response:
```json
{
  "errors": [{
    "message": "$openSearch cannot be combined with other filter operators. The $openSearch filter must be the only condition in the filter object.",
    "errorCode": "INVALID_OPEN_SEARCH_FILTER"
  }]
}
```

---

**`_id` filter — `term` on `_id` (Data API `_id` is rewritten to HCD composite-key encoding):**
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "term": { "_id": "product-3" }
      }
    }
  }
}
```
> The Data API rewrites this to `{"term": {"_id": "{_0=1, _1=product-3}"}}` before serialising the `expr(...)` predicate. The user always writes the plain Data API `_id` string; the encoding to the HCD composite-key format happens transparently server-side.

---

**`_id` filter — `ids` query for multiple documents (all values rewritten):**
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "ids": { "values": ["product-2", "product-3", "product-5"] }
      }
    }
  }
}
```
> Rewritten to `{"ids": {"values": ["{_0=1, _1=product-2}", "{_0=1, _1=product-3}", "{_0=1, _1=product-5}"]}}` before the `expr(...)` is assembled.

---

**`_id` filter inside `bool` — combined with full-text search:**
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "bool": {
          "must": [
            { "match": { "status": "pending" } }
          ],
          "filter": [
            { "terms": { "_id": ["product-2", "product-3"] } }
          ]
        }
      }
    }
  }
}
```
> The `terms` clause inside `bool.filter` is rewritten recursively: `{"terms": {"_id": ["{_0=1, _1=product-2}", "{_0=1, _1=product-3}"]}}`. The `match` clause is unchanged.

---

**Error case — `$openSearch` on a collection without OpenSearch enabled:**
```json
{
  "find": {
    "filter": {
      "$openSearch": { "match": { "name": "Widget" } }
    }
  }
}
```
Response:
```json
{
  "errors": [{
    "message": "The collection 'products' does not have an OpenSearch index. Enable openSearch at collection creation time to use the $openSearch filter.",
    "errorCode": "OPEN_SEARCH_NOT_ENABLED"
  }]
}
```

#### 4.4.4b Paging — sample payloads

The only supported paging mechanism is the standard Data API `pageState` token. `from`/`size` and `search_after` inside the `$openSearch` DSL are **not supported**.

**Page 1:**

```json
{ "find": {
    "filter": { "$openSearch": { "match": { "customerName": "Lovelace" } } },
    "options": { "limit": 20 }
} }
```

Response includes `nextPageState` when more results exist. Pass it back verbatim:

```json
{ "find": {
    "filter": { "$openSearch": { "match": { "customerName": "Lovelace" } } },
    "options": { "limit": 20, "pageState": "<token from previous response>" }
} }
```

---

#### 4.4.5 Implementation Tips (server)

**Behaviour rules**
- `$openSearch` is detected in [`CollectionFilterResolver`](src/main/java/io/stargate/sgv2/jsonapi/service/resolver/matcher/CollectionFilterResolver.java) when the filter object has `"$openSearch"` as its sole key. Detection must happen **before** the standard filter capture group matching, because `$openSearch`'s value is an arbitrary JSON object — not a primitive or a document-level comparison — and the existing match rules cannot handle it.
- When `$openSearch` is detected, the resolver first checks `collectionSchemaObject.openSearchDef().enabled()`. If `false` (or no OpenSearch def), throw `OPEN_SEARCH_NOT_ENABLED`.
- If any other key coexists with `"$openSearch"` in the filter object, throw `INVALID_OPEN_SEARCH_FILTER` before any index lookup.
- The `$openSearch` DSL JSON node is **rewritten** before serialisation (see the `_id` rewriting step below) and then serialised into `expr(<saiIndexName>, '<dsl>')`. Here `<saiIndexName>` is the **SAI index name** — the Cassandra custom index identifier stored in `collectionOpenSearchDef.saiIndexName` (auto-generated as `hcd_<keyspace>_<collectionName>`).
- A new `OpenSearchCollectionFilter` class is created (following [`MatchCollectionFilter`](src/main/java/io/stargate/sgv2/jsonapi/service/operation/filters/collection/MatchCollectionFilter.java)). Its `get()` method returns a `BuiltCondition` using a new `BuiltConditionPredicate.EXPR` predicate that renders as `expr(<saiIndexName>, '<dslJson>')`.
- [`FindCollectionOperation`](src/main/java/io/stargate/sgv2/jsonapi/service/operation/collections/FindCollectionOperation.java) does not need structural changes — `OpenSearchCollectionFilter` is added to the `DBLogicalExpression` like any other collection filter. The `EXPR` predicate handles the CQL generation.
- `sort` is allowed alongside `$openSearch` — the in-memory sort pipeline in `FindCollectionOperation` continues to operate after results are fetched from Cassandra, discarding OpenSearch's relevance ordering.
- Rerank must be explicitly blocked at the `findAndRerank` command level — see the "`findAndRerank` — `$openSearch` must be blocked at the outer command level" note below. For plain `find` with `$openSearch`, rerank is never triggered because the `$openSearch` branch in `FindCommandResolver` does not enter the rerank pipeline.

**`_id` DSL rewriting**

The Data API `_id` field is excluded from the OpenSearch `_source` (it is not in `propertiesFromJsonFields`) because OpenSearch permanently rejects `_id` as a body field. OpenSearch's reserved metadata `_id` instead holds the Cassandra composite primary-key encoding that HCD sets at index time: the string `{_0=<partitionKey>, _1=<clusteringKey>}`. A user writing `{"term": {"_id": "product-3"}}` in their `$openSearch` DSL would never get a match — the OS `_id` for that document is `{_0=1, _1=product-3}`.

To fix this transparently, a new `OpenSearchDslIdRewriter` (or equivalent utility method on `OpenSearchCollectionFilter`) performs a recursive walk of the DSL `JsonNode` before it is serialised, applying the following rewrites:

| Clause found | Rewriting rule |
|---|---|
| `{"term": {"_id": "<value>"}}` | Replace string value with `OpenSearchIdEncoder.encode("<value>")` → `"{_0=1, _1=<value>}"` |
| `{"terms": {"_id": ["<v1>", "<v2>", ...]}}` | Replace each string element with the encoded form |
| `{"ids": {"values": ["<v1>", "<v2>", ...]}}` | Replace each string element with the encoded form |
| Any of the above nested inside `bool.must`, `bool.should`, `bool.must_not`, `bool.filter` arrays | Same rules applied recursively |
| All other clauses (`match`, `range`, `wildcard`, `fuzzy`, `multi_match`, etc.) | No change — passed through unchanged |

`OpenSearchIdEncoder.encode(String dataApiId)` constructs the HCD composite-key string. For collections the partition key is always `1` (a fixed integer — collections use a single-partition shredded schema) and the clustering key is the user `_id` value, so the encoded form is always `{_0=1, _1=<dataApiId>}`.

> **`search_after` cursor with `_id` tiebreaker is NOT rewritten.** When a client sorts by `_id` inside the DSL and uses `search_after`, the cursor value returned by OpenSearch is already in the `{_0=1, _1=...}` encoding. The client echoes it verbatim — the Data API must not rewrite `search_after` array values.

**Paging implementation notes**
- **`pageState` paging:** the standard Cassandra paging flow works unchanged — `OpenSearchCollectionFilter` produces a `WHERE expr(...)` clause and Cassandra pages through the result rows normally. `options.limit` is emitted as a CQL `LIMIT` and works correctly here. No server-side changes required.
- **`from`/`size` inside the DSL:** these keys are part of the `$openSearch` JSON value and are passed verbatim to OpenSearch via `expr(...)`. The Data API does not inspect or transform them. OpenSearch resolves the window before returning PKs to Cassandra. **The Data API must not emit a CQL `LIMIT`** alongside a DSL `size` — HCD rejects this combination. Validate at the resolver: if the `$openSearch` value contains a top-level `"size"` key, reject any `options.limit` with `INVALID_OPEN_SEARCH_FILTER` (or a dedicated error). No other server-side changes required.
- **`search_after` inside the DSL:** same pass-through as `from`/`size`. The `search_after` array and the DSL `sort` array are part of the `$openSearch` JSON and reach OpenSearch unmodified. The server must **not** strip or reorder keys in the DSL object. No additional server-side changes required.
- **`source_only` option:** HCD supports a `source_only: true` prefix token in the `expr(...)` envelope that makes the query handler return results built directly from the OpenSearch document source, bypassing Cassandra replica reads entirely. **This option is not useful for collections** — `doc_json` is never replicated to OpenSearch (only the individual indexed fields are), so a source-only query on a collection would return incomplete documents. The Data API does not expose `source_only` for collections. For tables, where all replicated columns are available in the OpenSearch source, it could be a future performance option — deferred.

**`$openSearch` on write commands (findOneAndUpdate, updateOne, deleteOne, etc.)**

> **Scope of `$openSearch` on filter-accepting commands.** §4.4.1 states that `$openSearch` applies to all commands that accept a filter. Each of those command resolvers ultimately delegates filter resolution to `CollectionFilterResolver`. As long as the early-intercept in item 3 below is in place, all such resolvers will naturally inherit `$openSearch` support — no additional resolver changes are needed per command. The list of commands that benefit automatically: `findOne`, `findOneAndUpdate`, `findOneAndReplace`, `findOneAndDelete`, `updateOne`, `updateMany`, `deleteOne`, `deleteMany`.

**`findAndRerank` — `$openSearch` must be blocked at the outer command level**

> **Risk:** `FindAndRerankCommand` constructs an inner `FindCommand` using the user-supplied `filterDefinition` verbatim (via `FindAndRerankOperationBuilder.buildBm25Read()` / `buildVectorRead()`). If a user passes `$openSearch` as the outer filter to `findAndRerank`, it flows into the inner `FindCommand` unchecked, reaches `CollectionFilterResolver`, and either fails to match any capture group (producing empty results or an error) or — if the early-intercept is added — routes to `OpenSearchCollectionFilter` inside a rerank context for which no result handler exists. Both outcomes are wrong.
>
> **Required guard:** in `FindAndRerankCommandResolver`, **before** delegating to `FindAndRerankOperationBuilder`, check whether `command.filterDefinition()` contains a `"$openSearch"` key. If present, throw `RERANK_NOT_SUPPORTED_WITH_OPEN_SEARCH` immediately. This block must live at the **outer** command level — not inside `FindCommandResolver` — because `FindAndRerankOperationBuilder` builds the inner `FindCommand` before `FindCommandResolver` ever sees it.

**Todo list**
1. Create `OpenSearchCollectionFilter` in `service/operation/filters/collection` — constructor takes `String saiIndexName` (the Cassandra custom index identifier, e.g. `hcd_mykeyspace_products`) and `JsonNode queryDsl`; `get()` builds `expr(saiIndexName, dslJson)` using a new `EXPR` predicate.
2. Add `EXPR` to `BuiltConditionPredicate` (or reuse a suitable existing predicate) to emit the `expr(...)` CQL form.
3. Intercept before `buildMatchRules` executes in [`CollectionFilterResolver`](src/main/java/io/stargate/sgv2/jsonapi/service/resolver/matcher/CollectionFilterResolver.java) — check `filter.fields()` for `"$openSearch"` and return early with an `OpenSearchCollectionFilter` if found. This single intercept point covers `find`, `findOne`, `findOneAndUpdate`, `findOneAndReplace`, `findOneAndDelete`, `updateOne`, `updateMany`, `deleteOne`, `deleteMany` automatically — all of them use `CollectionFilterResolver`.
4. Add validation: if `$openSearch` key present with other keys → `INVALID_OPEN_SEARCH_FILTER`; if `openSearchDef.enabled == false` → `OPEN_SEARCH_NOT_ENABLED`.
5. Create `OpenSearchDslIdRewriter` utility class (or static method on `OpenSearchCollectionFilter`) — performs a recursive `JsonNode` walk of the DSL before serialisation: for `term._id`, `terms._id`, and `ids.values`, replace each string value with `OpenSearchIdEncoder.encode(value)`. Create `OpenSearchIdEncoder` with a single `encode(String dataApiId)` method that produces `"{_0=1, _1=<dataApiId>}"`. The rewriter must handle these clauses at any depth inside `bool.must`, `bool.should`, `bool.must_not`, and `bool.filter` arrays. **Do not rewrite `search_after` array values.**
6. Update [`FindCommandResolver.resolveCollectionCommand()`](src/main/java/io/stargate/sgv2/jsonapi/service/resolver/FindCommandResolver.java) to detect the `OpenSearchCollectionFilter` in the resolved expression and route to `FindCollectionOperation.openSearch(...)` factory method (or reuse `unsortedSingle` / `unsorted` with a flag).
7. Block rerank + `$openSearch` at the **outer command level**: in `FindAndRerankCommandResolver`, check `command.filterDefinition()` for `"$openSearch"` before delegating to `FindAndRerankOperationBuilder`. Throw `RERANK_NOT_SUPPORTED_WITH_OPEN_SEARCH`. This must NOT be inside `FindCommandResolver` — the inner `FindCommand` is already built by the time `FindCommandResolver` is called.
8. Add `RERANK_NOT_SUPPORTED_WITH_OPEN_SEARCH` to the error code enum.

**Relevant context**
- Filter pattern: [`MatchCollectionFilter`](src/main/java/io/stargate/sgv2/jsonapi/service/operation/filters/collection/MatchCollectionFilter.java)
- Filter resolver: [`CollectionFilterResolver`](src/main/java/io/stargate/sgv2/jsonapi/service/resolver/matcher/CollectionFilterResolver.java) — `buildMatchRules()` and `findDynamic()` pattern for capture group dispatch
- Command resolver: [`FindCommandResolver.resolveCollectionCommand()`](src/main/java/io/stargate/sgv2/jsonapi/service/resolver/FindCommandResolver.java) — routing branch between `vsearch`, `bm25Multi`, `sorted`, and normal reads
- Operation: [`FindCollectionOperation`](src/main/java/io/stargate/sgv2/jsonapi/service/operation/collections/FindCollectionOperation.java) — static factory methods pattern

#### 4.4.6 Implementation Tips (clients)

> Client SDK documentation: [HCD API Reference — Instantiate Client](https://docs.datastax.com/en/hyper-converged-database/2.0/api-reference/instantiate-client.html) and sub-pages.

> **DSL `size` vs `options.limit` — they are mutually exclusive.** Use `"size": N` inside the `$openSearch` DSL to control how many hits OpenSearch fetches. **Never set `options.limit` at the same time** — HCD rejects a CQL `LIMIT` alongside a DSL `size` with an explicit error. Only use `options.limit` when doing `pageState`-based paging (no `size` key in the DSL).

**Java — `pageState` paging (relevance order)**
```java
import static io.stargate.sgv2.jsonapi.api.client.search.OpenSearchQuery.*;

// Page 1
FindIterable<Document> page1 = collection.find(
    Filters.search(match("description", "cassandra database")),
    new FindOptions().limit(20)
);
String nextPageState = page1.getNextPageState(); // null if no more pages

// Page 2
collection.find(
    Filters.search(match("description", "cassandra database")),
    new FindOptions().limit(20).pageState(nextPageState)
);
```

**Java — `from`/`size` DSL paging (offset-based, client tracks page number)**
```java
int pageSize = 20;
int pageNumber = 0; // 0-indexed

// size in the DSL is the hit bound — do NOT pass FindOptions.limit() alongside it
var dsl = OpenSearchQuery.builder()
    .from(pageSize * pageNumber)
    .size(pageSize)
    .query(match("description", "cassandra database"))
    .build();
collection.find(Filters.search(dsl)); // no limit() — size in DSL controls the result count

// Next page: rebuild with incremented pageNumber
```

**Java — `search_after` deep paging (unlimited results, requires sort tiebreaker)**
```java
// Page 1 — no search_after yet; size in DSL bounds the page — do NOT pass limit()
var dsl = OpenSearchQuery.builder()
    .size(20)
    .query(match("description", "cassandra database"))
    .sort(Sort.byScore("desc"), Sort.by("_id", "asc"))
    .build();
List<Document> docs = collection.find(Filters.search(dsl))
    .into(new ArrayList<>());

// Page 2 — pass last hit's sort values as search_after
// The sort values come from the last document's _sortValues (SDK-dependent field)
List<Object> sortValues = docs.get(docs.size() - 1).getSortValues(); // [0.87, "doc-abc123"]
var dslPage2 = OpenSearchQuery.builder()
    .size(20)
    .query(match("description", "cassandra database"))
    .sort(Sort.byScore("desc"), Sort.by("_id", "asc"))
    .searchAfter(sortValues)
    .build();
collection.find(Filters.search(dslPage2)); // no limit()
```

---

**Python — `pageState` paging (relevance order)**
```python
# Page 1
cursor = collection.find(
    {"$openSearch": {"match": {"description": "cassandra database"}}},
    limit=20
)
docs_page1 = list(cursor)
next_page_state = cursor.next_page_state  # None if no more pages

# Page 2
collection.find(
    {"$openSearch": {"match": {"description": "cassandra database"}}},
    limit=20,
    page_state=next_page_state
)
```

**Python — `from`/`size` DSL paging**
```python
page_size = 20

# size in the DSL is the hit bound — do NOT pass limit= alongside it
for page_num in range(5):  # first 5 pages
    results = list(collection.find({
        "$openSearch": {
            "from": page_size * page_num,
            "size": page_size,
            "query": {"match": {"description": "cassandra database"}}
        }
    }))  # no limit= argument
    if not results:
        break
    process(results)
```

**Python — `search_after` deep paging**
```python
page_size = 20
search_after = None  # start from the beginning

while True:
    dsl = {
        "size": page_size,  # size in DSL bounds the page — do NOT pass limit=
        "query": {"match": {"description": "cassandra database"}},
        "sort": [{"_score": {"order": "desc"}}, {"_id": {"order": "asc"}}]
    }
    if search_after:
        dsl["search_after"] = search_after

    docs = list(collection.find({"$openSearch": dsl}))  # no limit= argument
    if not docs:
        break
    process(docs)
    # Extract sort values from the last document.
    # The SDK must surface the raw OpenSearch hit metadata (sort values array).
    # Exact attribute name is SDK-specific — consult your SDK's search_after docs.
    # Example: docs[-1].get("_sort_values") or docs[-1]["_meta"]["sort_values"]
    search_after = docs[-1].get("_sort_values")  # replace with actual SDK attribute
```

---

**TypeScript / Node.js — `pageState` paging (relevance order)**
```typescript
// Page 1
const cursor = collection.find({ $openSearch: { match: { description: "cassandra database" } } });
const docs1 = await cursor.toArray();          // fetches first page (default limit)
const nextPageState = cursor.nextPageState();  // undefined if no more pages

// Page 2
const cursor2 = collection.find(
  { $openSearch: { match: { description: "cassandra database" } } },
  { pageState: nextPageState }
);
const docs2 = await cursor2.toArray();
```

**TypeScript / Node.js — `from`/`size` DSL paging**
```typescript
const pageSize = 20;

// size in the DSL is the hit bound — do NOT pass a limit option alongside it
async function fetchPage(pageNum: number) {
  return collection.find({
    $openSearch: {
      from: pageSize * pageNum,
      size: pageSize,
      query: { match: { description: "cassandra database" } }
    }
  }).toArray(); // no limit option
}

// Iterate pages
for (let page = 0; ; page++) {
  const docs = await fetchPage(page);
  if (docs.length === 0) break;
  process(docs);
}
```

**TypeScript / Node.js — `search_after` deep paging**
```typescript
const pageSize = 20;
let searchAfter: unknown[] | undefined;

while (true) {
  const dsl: Record<string, unknown> = {
    size: pageSize,  // size in DSL bounds the page — do NOT pass a limit option
    query: { match: { description: "cassandra database" } },
    sort: [{ _score: { order: "desc" } }, { _id: { order: "asc" } }]
  };
  if (searchAfter) dsl.search_after = searchAfter;

  const docs = await collection.find({ $openSearch: dsl }).toArray(); // no limit option
  if (docs.length === 0) break;
  process(docs);
  // Extract sort values from the last doc (SDK-dependent, raw OpenSearch hit metadata).
  // Exact property name depends on the SDK version — consult your SDK's search_after docs.
  searchAfter = (docs[docs.length - 1] as any)._sortValues; // replace with actual SDK property
}
```

---

### 4.5 `find` on Tables — new `$openSearch` filter operator

#### 4.5.1 Goal

Tables that have a `createOpenSearchIndex` index (§4.3) gain the same `$openSearch` filter operator as collections. The behaviour and constraints are identical — OpenSearch DSL object as the filter value, exclusive filter key, same error codes. The only difference is the routing path inside the Data API: table reads go through `WhereCQLClauseAnalyzer` rather than the collection filter resolver.

**Default result order is OpenSearch relevance.** When no `sort` clause is provided, rows are returned in the order OpenSearch produces them — descending by relevance score. Adding a `sort` clause overrides this with an in-memory sort on column values, which discards the OpenSearch relevance ordering.

**Paging — all three strategies from §4.4.1 apply identically to tables.** Because `$openSearch` passes the DSL verbatim to `expr(...)` for both collections and tables, `from`/`size`, `search_after`, and `pageState` all work the same way. The sort in `search_after` queries must reference OpenSearch field names that correspond to **table column names** (not document property names as in collections). See §4.4.1 for the full strategy comparison table.

#### 4.5.2 How it works

The behaviour and constraints for tables are identical to collections (§4.4.2) — `$openSearch` must be the sole filter key, it routes the DSL through `expr(...)` to HCD, and `sort` + `projection` work as normal. The only difference is the internal routing path: table reads go through `TableFilterResolver` and `WhereCQLClauseAnalyzer` rather than the collection filter resolver.

**Sequence diagram — request flow:**

```mermaid
sequenceDiagram
    participant APP as Application
    participant API as Data API
    participant CQL as CQL / Cassandra
    participant OQH as OpenSearchQueryHandler
    participant OS as OpenSearch

    APP->>API: find\nfilter: {$openSearch: {bool: {must: [{match: {title: cassandra}}],\nfilter: [{term: {status: published}}]}}}
    API->>API: WhereCQLClauseAnalyzer detects $openSearch key\nlookup ApiOpenSearchIndex from TableSchemaObject\nreads saiIndexName (= articles_search)\nbuilds OpenSearchTableFilter
    API->>CQL: SELECT * FROM articles\nWHERE expr(articles_search,\n{"bool":{...}})\n(articles_search = SAI index name)
    CQL->>OQH: intercept expr() predicate\nresolves SAI index → OS index name (articles-ft)
    OQH->>OS: POST /articles-ft/_search\n(articles-ft = OS index name)\n{bool query DSL}
    OS-->>OQH: hits: [{_id:...}, ...]
    OQH-->>CQL: Cassandra rows for matching PKs
    CQL-->>API: rows
    API->>API: apply projection
    API-->>APP: {data: {documents: [{...}]}}
```

#### 4.5.3 OpenSearch fields in the Data API Payload

The `$openSearch` filter value for tables is the same verbatim **OpenSearch Query DSL** object as for collections — see [§4.4.3](#443-opensearch-fields-in-the-data-api-payload) for the full field reference and supported query types. The only difference is that the DSL is matched against the replicated **table columns** (not unpacked JSON document fields), so field names in the DSL must match the actual column names of the table.

#### 4.5.4 Sample payloads

**Paging — `from`/`size` on a table (offset paging):**
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "from": 0,
        "size": 20,
        "query": {
          "match": { "body": "distributed tracing observability" }
        }
      }
    }
  }
}
```
Page 2 — increment `from`:
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "from": 20,
        "size": 20,
        "query": {
          "match": { "body": "distributed tracing observability" }
        }
      }
    }
  }
}
```

---

**Paging — `search_after` on a table (deep paging, sort by column values):**

Page 1 — sort by a score + unique column tiebreaker:
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "size": 20,
        "query": {
          "match": { "body": "distributed tracing observability" }
        },
        "sort": [
          { "_score":     { "order": "desc" } },
          { "article_id": { "order": "asc"  } }
        ]
      }
    }
  }
}
```
Page 2 — pass the last document's sort values as `search_after`:
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "size": 20,
        "query": {
          "match": { "body": "distributed tracing observability" }
        },
        "sort": [
          { "_score":     { "order": "desc" } },
          { "article_id": { "order": "asc"  } }
        ],
        "search_after": [0.91, 42]
      }
    }
  }
}
```
→ CQL emitted for both pages:
```cql
-- articles_search = SAI index name (user-supplied via createOpenSearchIndex.name)
SELECT * FROM mykeyspace.articles
WHERE expr(articles_search, '{"size":20,"query":{"match":{"body":"..."}},"sort":[...],"search_after":[...]}')
```

> The `sort` inside the DSL references **table column names** (`article_id`). The Data API does not transform or validate these names — they must match the column names as they appear in the OpenSearch index. No Data API `pageState` is needed for `search_after` paging; the client maintains the cursor from the last document's sort values.

---

**Simple full-text search on a table:**
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "match": { "body": "distributed tracing observability" }
      }
    }
  }
}
```
→ CQL emitted:
```cql
-- articles_search = SAI index name (user-supplied via createOpenSearchIndex.name)
SELECT * FROM mykeyspace.articles
WHERE expr(articles_search, '{"match":{"body":"distributed tracing observability"}}')
```

---

**Multi-field search with projection:**
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "multi_match": {
          "query": "database performance tuning",
          "fields": ["title", "body", "tags"]
        }
      }
    },
    "projection": { "title": 1, "author": 1, "published_at": 1 },
    "options": { "limit": 10 }
  }
}
```

---

**Boolean query with must + filter clauses:**
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "bool": {
          "must": [
            { "match": { "title": "cassandra" } }
          ],
          "filter": [
            { "term": { "status": "published" } },
            { "range": { "view_count": { "gte": 1000 } } }
          ],
          "must_not": [
            { "term": { "category": "deprecated" } }
          ]
        }
      }
    },
    "options": { "limit": 25 }
  }
}
```

---

**Phrase match on a table column:**
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "match_phrase": { "description": "real time analytics" }
      }
    }
  }
}
```

---

**Fuzzy search (OpenSearch DSL):**
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "fuzzy": {
          "title": {
            "value": "casandra",
            "fuzziness": "AUTO"
          }
        }
      }
    }
  }
}
```

---

**`findOne` — first matching document:**
```json
{
  "findOne": {
    "filter": {
      "$openSearch": {
        "term": { "author_id": "user-42" }
      }
    }
  }
}
```

---

**`updateOne` via `findOneAndUpdate` on a table:**
```json
{
  "findOneAndUpdate": {
    "filter": {
      "$openSearch": {
        "match": { "title": "Introduction to CQL" }
      }
    },
    "update": {
      "$set": { "status": "archived" }
    }
  }
}
```

---

**k-NN vector search on a table — pass-through for advanced users who configure a `knn_vector` column mapping:**
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "knn": {
          "article_embedding": {
            "vector": [0.22, 0.71, 0.45, 0.88],
            "k": 10
          }
        }
      }
    }
  }
}
```
> **Prerequisite:** the table's OpenSearch index must declare the `article_embedding` column as `"type": "knn_vector"` in `createOpenSearchIndex.definition.options.mappings`. The `vector` array length must match the declared dimension. This is a pass-through pattern — no Data API validation is performed on the DSL payload.

---

**`neural` dense vector query on a table — OpenSearch model-backed embedding lookup:**
```json
{
  "find": {
    "filter": {
      "$openSearch": {
        "neural": {
          "article_embedding": {
            "query_text": "real-time analytics with Cassandra",
            "model_id": "aB3dEfGh",
            "k": 5
          }
        }
      }
    }
  }
}
```

---

**Error case — `$openSearch` combined with a column filter on a table:**
```json
{
  "find": {
    "filter": {
      "$openSearch": { "match": { "title": "tutorial" } },
      "status": "published"
    }
  }
}
```
Response:
```json
{
  "errors": [{
    "message": "$openSearch cannot be combined with other filter operators. The $openSearch filter must be the only condition in the filter object.",
    "errorCode": "INVALID_OPEN_SEARCH_FILTER"
  }]
}
```

---

**Error case — `$openSearch` on a table with no OpenSearch index:**
```json
{
  "find": {
    "filter": {
      "$openSearch": { "match": { "title": "tutorial" } }
    }
  }
}
```
Response:
```json
{
  "errors": [{
    "message": "Table 'articles' has no OpenSearch index. Use createOpenSearchIndex to create one before using the $openSearch filter.",
    "errorCode": "OPEN_SEARCH_NOT_ENABLED"
  }]
}
```

#### 4.5.5 Implementation Tips (server)

**Behaviour rules**
- `$openSearch` detection for tables follows the same pre-validation pattern as for collections, but occurs inside [`TableFilterResolver`](src/main/java/io/stargate/sgv2/jsonapi/service/resolver/matcher/TableFilterResolver.java) rather than `CollectionFilterResolver`.
- Before any column-level filter resolution, check whether the filter has `"$openSearch"` as its sole key. If other keys coexist, throw `INVALID_OPEN_SEARCH_FILTER`.
- Look up `ApiOpenSearchIndex` from `tableSchemaObject.apiTableDef().indexes()` by type `OPEN_SEARCH`. If none exists, throw `OPEN_SEARCH_NOT_ENABLED`.
- Build a new `OpenSearchTableFilter` (analogous to `OpenSearchCollectionFilter` but implementing `TableFilter` / `WhereCQLClause<Select>`) that wraps the DSL JSON and the **SAI index name** recovered from `ApiOpenSearchIndex.saiIndexName()`.
- `OpenSearchTableFilter.buildStatement()` renders `WHERE expr(<saiIndexName>, '<dslJson>')` using the QueryBuilder `raw()` literal or a custom `ExpressionCQLClause`.
- Pass the `OpenSearchTableFilter` as the sole where clause to [`TableReadDBTaskBuilder.build()`](src/main/java/io/stargate/sgv2/jsonapi/service/operation/tables/TableReadDBTaskBuilder.java). No `WhereCQLClauseAnalyzer` validation runs for `$openSearch` — the analyzer's checks (PK presence, ALLOW FILTERING warnings) are not relevant for an `expr(...)` predicate.
- `sort` and `projection` work as normal — no special handling required beyond skipping the standard `WhereCQLClauseAnalyzer`.

**Todo list**
1. Create `OpenSearchTableFilter` in `service/operation/filters/table` — implements `TableFilter`; constructor takes `String saiIndexName` (the Cassandra custom index identifier, from `ApiOpenSearchIndex.saiIndexName()`) and `JsonNode queryDsl`; renders `WHERE expr(<saiIndexName>, '<dslJson>')`.
2. Add `$openSearch` interception logic to [`TableFilterResolver`](src/main/java/io/stargate/sgv2/jsonapi/service/resolver/matcher/TableFilterResolver.java) (this runs inside `resolve(...)`, so `find`, `findOne`, `updateOne`, `deleteOne` and `deleteMany` all share it) — check for `"$openSearch"` in the filter before standard column matching; validate exclusivity; look up `ApiOpenSearchIndex`; construct `OpenSearchTableFilter`.
3. Update [`WhereCQLClauseAnalyzer.StatementType.SELECT`](src/main/java/io/stargate/sgv2/jsonapi/service/operation/tables/WhereCQLClauseAnalyzer.java) strategy to short-circuit all validation rules when the where clause contains an `OpenSearchTableFilter` — the standard PK and indexing checks do not apply.
4. Update `TableReadDBOperationBuilder` to bypass the `WhereCQLClauseAnalyzer` (or pass through a no-op strategy) when an `OpenSearchTableFilter` is detected.
5. Add `OPEN_SEARCH_NOT_ENABLED` and `INVALID_OPEN_SEARCH_FILTER` to table-path error code enum if not already shared with the collection path.

**Relevant context**
- Collection parallel: `OpenSearchCollectionFilter` (new, see §4.4.5)
- Table filter pattern: [`NativeTypeTableFilter`](src/main/java/io/stargate/sgv2/jsonapi/service/operation/filters/table/NativeTypeTableFilter.java)
- Table filter resolver: [`TableFilterResolver`](src/main/java/io/stargate/sgv2/jsonapi/service/resolver/matcher/TableFilterResolver.java) — analogue of `CollectionFilterResolver` for table reads
- Where clause analyzer: [`WhereCQLClauseAnalyzer`](src/main/java/io/stargate/sgv2/jsonapi/service/operation/tables/WhereCQLClauseAnalyzer.java) — `checkLexicalFilterOnlyOnLexicalIndex` is the closest existing analogous guard; `$openSearch` needs a similar pre-check that replaces standard analysis
- Task builder: [`TableReadDBTaskBuilder`](src/main/java/io/stargate/sgv2/jsonapi/service/operation/tables/TableReadDBTaskBuilder.java) — `build(WhereCQLClause<Select>)` called with the OpenSearch where clause
- Operation builder: [`TableReadDBOperationBuilder`](src/main/java/io/stargate/sgv2/jsonapi/service/resolver/TableReadDBOperationBuilder.java) — orchestrates filter resolution, sort, and task construction

#### 4.5.6 Implementation Tips (clients)

> Client SDK documentation: [HCD API Reference — Instantiate Client](https://docs.datastax.com/en/hyper-converged-database/2.0/api-reference/instantiate-client.html) and sub-pages.

**Java**
```java
import static io.stargate.sgv2.jsonapi.api.client.search.OpenSearchQuery.*;

// Simple full-text search on a table
table.find(
    Filters.search(match("body", "distributed tracing observability"))
);

// Boolean query with projection and limit
table.find(
    Filters.search(
        OpenSearchQuery.builder()
            .bool()
                .must(match("title", "cassandra"))
                .filter(term("status", "published"))
            .build()
    ),
    new FindOptions().projection(Projection.include("title", "author", "published_at")).limit(25)
);

// search_after deep paging on a table (sort by table column as tiebreaker)
// size in DSL bounds the page — do NOT pass FindOptions.limit() alongside it
var dsl = OpenSearchQuery.builder()
    .size(20)
    .query(match("body", "distributed tracing observability"))
    .sort(Sort.byScore("desc"), Sort.by("article_id", "asc"))
    .build();
List<Row> page1 = table.find(Filters.search(dsl)).into(new ArrayList<>()); // no limit()

// Page 2 — pass last row's sort values
List<Object> sortValues = page1.get(page1.size() - 1).getSortValues(); // [0.91, 42]
var dslPage2 = OpenSearchQuery.builder()
    .size(20)
    .query(match("body", "distributed tracing observability"))
    .sort(Sort.byScore("desc"), Sort.by("article_id", "asc"))
    .searchAfter(sortValues)
    .build();
table.find(Filters.search(dslPage2)); // no limit()
```

**Python**
```python
# Simple full-text search on a table
table.find({"$openSearch": {"match": {"body": "distributed tracing observability"}}})

# Boolean query with projection
table.find(
    {
        "$openSearch": {
            "bool": {
                "must": [{"match": {"title": "cassandra"}}],
                "filter": [{"term": {"status": "published"}}]
            }
        }
    },
    projection={"title": True, "author": True, "published_at": True},
    limit=25
)
```

**TypeScript / Node.js**
```typescript
// Simple full-text search on a table
await table.find({
  $openSearch: { match: { body: "distributed tracing observability" } }
}).toArray();

// Boolean query with projection and limit
await table.find(
  {
    $openSearch: {
      bool: {
        must: [{ match: { title: "cassandra" } }],
        filter: [{ term: { status: "published" } }]
      }
    }
  },
  { projection: { title: 1, author: 1, published_at: 1 }, limit: 25 }
).toArray();
```

---

---

### 4.6 `searchAggregate` — aggregations and faceted search

#### 4.6.1 Goal

`searchAggregate` is a new dedicated command that runs OpenSearch aggregation queries against a collection or table that has an OpenSearch index configured. It covers the full spectrum of analytical use cases: faceted navigation (e.g. "filter by category with counts"), histograms, date bucketing, stats (avg/sum/min/max/cardinality), and nested `top_hits` — all in a single API call.

The aggregation DSL is passed verbatim to `expr(...)` as a full OpenSearch search request body containing `aggs`. An optional `query` key scopes the aggregation to a matching subset of documents.

The command throws `OPEN_SEARCH_NOT_ENABLED` if the collection or table does not have an OpenSearch index. The response shape is **not** a document list — it is a flat map of aggregation-name → JSON result, matching exactly what OpenSearch returns.

#### 4.6.2 How it works

```mermaid
sequenceDiagram
    participant APP as Application
    participant API as Data API
    participant CQL as CQL / Cassandra
    participant OQH as OpenSearchQueryHandler
    participant OS as OpenSearch

    APP->>API: searchAggregate<br/>aggregations:{by_cat:{terms:{field:category.keyword}}}<br/>query:{match:{description:cassandra}}
    API->>API: validate openSearch enabled<br/>build aggs+query body<br/>serialise to expr() DSL
    API->>CQL: SELECT * FROM products<br/>WHERE expr(hcd_ks_products, {"query":...,"aggs":{...}})
    CQL->>OQH: intercept expr() — detects aggs key<br/>switch to aggregation mode
    OQH->>OS: POST /products-search/_search {query+aggs body}
    OS-->>OQH: aggregation result JSON
    OQH-->>CQL: 1 row, 1 text column per top-level agg
    CQL-->>API: aggregation row
    API->>API: deserialise each column as JSON<br/>build status.aggregations map
    API-->>APP: {status:{aggregations:{by_cat:{buckets:[...]}}}}
```

#### 4.6.3 OpenSearch fields in the Data API Payload

**Request fields:**

| Field | Required | Description |
|---|---|---|
| `aggregations` | **yes** | Map of aggregation name → OpenSearch aggregation definition. Any OpenSearch aggregation type is supported: `terms`, `date_histogram`, `range`, `avg`, `sum`, `min`, `max`, `cardinality`, `top_hits`, nested, pipeline, etc. |
| `query` | no | OpenSearch query DSL to scope the aggregation. Omit to aggregate over all indexed documents. Same syntax as the `$openSearch` filter value. |

**Response fields (`status.aggregations`):**

| Field | Type | Description |
|---|---|---|
| `status.aggregations` | Object | Map of aggregation name → raw OpenSearch aggregation result. Each value is the JSON subtree returned by OpenSearch for that aggregation. Structure depends on the aggregation type. |
| `status.aggregations.<name>.buckets` | Array | For bucket aggregations (`terms`, `date_histogram`, `range`). Each bucket has `key`, `doc_count`, and any nested aggregation results. |
| `status.aggregations.<name>.value` | Number | For metric aggregations (`avg`, `sum`, `min`, `max`, `cardinality`). |

> **`OPEN_SEARCH_NOT_ENABLED`**: thrown when `searchAggregate` is called on a collection that was not created with `openSearch.enabled: true`, or on a table with no `createOpenSearchIndex`. Error message identifies whether the resource is a collection or table.

> **`OPEN_SEARCH_AGGREGATION_TOO_LARGE`**: thrown when the OpenSearch response for a single aggregation exceeds the `max_agg_json_bytes` limit (default 1 MiB) or the full response exceeds `max_agg_response_bytes` (default 16 MiB). These bounds are enforced by `OpenSearchQueryHandler` before the result reaches the Data API. Reduce aggregation scope (e.g. lower `size` on `terms`) or add a `query` to narrow the dataset.

#### 4.6.4 Sample payloads

---

**Faceted navigation — category counts + price range buckets:**
```json
{
  "searchAggregate": {
    "query": {
      "match": { "description": "cassandra database" }
    },
    "aggregations": {
      "by_category": {
        "terms": { "field": "category.keyword", "size": 10 }
      },
      "price_ranges": {
        "range": {
          "field": "price",
          "ranges": [
            { "to": 25 },
            { "from": 25, "to": 100 },
            { "from": 100 }
          ]
        }
      }
    }
  }
}
```
Response:
```json
{
  "status": {
    "aggregations": {
      "by_category": {
        "buckets": [
          { "key": "database",    "doc_count": 42 },
          { "key": "distributed", "doc_count": 18 }
        ]
      },
      "price_ranges": {
        "buckets": [
          { "key": "*-25.0",    "to": 25,  "doc_count": 7  },
          { "key": "25.0-100.0","from": 25,"to": 100,"doc_count": 28 },
          { "key": "100.0-*",   "from": 100,           "doc_count": 13 }
        ]
      }
    }
  }
}
```

---

**Date histogram + nested avg — articles per month with average word count:**
```json
{
  "searchAggregate": {
    "aggregations": {
      "articles_per_month": {
        "date_histogram": {
          "field": "published_at",
          "calendar_interval": "month"
        },
        "aggs": {
          "avg_word_count": { "avg": { "field": "word_count" } }
        }
      }
    }
  }
}
```
Response:
```json
{
  "status": {
    "aggregations": {
      "articles_per_month": {
        "buckets": [
          { "key_as_string": "2024-11-01", "key": 1730419200000, "doc_count": 14,
            "avg_word_count": { "value": 823.5 } },
          { "key_as_string": "2024-12-01", "key": 1733011200000, "doc_count": 22,
            "avg_word_count": { "value": 1102.0 } }
        ]
      }
    }
  }
}
```

---

**Multiple metric aggregations — total revenue + unique customer count:**
```json
{
  "searchAggregate": {
    "query": {
      "term": { "status": "completed" }
    },
    "aggregations": {
      "total_revenue":    { "sum":         { "field": "amount" } },
      "avg_order_value":  { "avg":         { "field": "amount" } },
      "unique_customers": { "cardinality": { "field": "customer_id.keyword" } }
    }
  }
}
```
Response:
```json
{
  "status": {
    "aggregations": {
      "total_revenue":    { "value": 48293.50 },
      "avg_order_value":  { "value": 127.35 },
      "unique_customers": { "value": 379 }
    }
  }
}
```

---

**Error — `searchAggregate` on a collection without OpenSearch:**
```json
{
  "searchAggregate": {
    "aggregations": {
      "by_category": { "terms": { "field": "category.keyword" } }
    }
  }
}
```
Response:
```json
{
  "errors": [{
    "message": "The collection 'products' does not have an OpenSearch index. Enable openSearch at collection creation time to use searchAggregate.",
    "errorCode": "OPEN_SEARCH_NOT_ENABLED"
  }]
}
```

---

**Error — aggregation response too large:**
```json
{
  "errors": [{
    "message": "The aggregation 'all_terms' response exceeds the maximum allowed size (1 MiB). Reduce the aggregation 'size' parameter or narrow the query scope.",
    "errorCode": "OPEN_SEARCH_AGGREGATION_TOO_LARGE"
  }]
}
```

#### 4.6.5 Implementation Tips (server)

**Behaviour rules**
- `searchAggregate` is a new collection-level and table-level command, registered in the command router alongside `find` and `countDocuments`.
- On collections: validate `collectionSchemaObject.openSearchDef().enabled()`. If `false`, throw `OPEN_SEARCH_NOT_ENABLED` with a message naming the collection.
- On tables: look up `ApiOpenSearchIndex` from `tableSchemaObject.apiTableDef().indexes()`. If absent, throw `OPEN_SEARCH_NOT_ENABLED` with a message naming the table.
- Build the OpenSearch search request body: `{ "query": <optional>, "aggs": <required> }` and serialise to JSON string.
- Emit: `SELECT * FROM <table> WHERE expr(<saiIndexName>, '<aggs-body>')`, where `<saiIndexName>` is the **SAI index name** (auto-generated for collections; user-supplied for tables).
- `OpenSearchQueryHandler` switches to aggregation mode when it sees an `aggs` key. It returns **one CQL row** with one `text` column per top-level aggregation name.
- The Data API reads each column as a JSON string, parses it, and builds the `status.aggregations` response map. Keys are the aggregation names as specified by the client — verbatim, not lowercased.
- Map the two HCD size errors (`OPEN_SEARCH_AGGREGATION_TOO_LARGE`) from `InvalidRequestException` messages that contain the bound strings.
- `LIMIT` is rejected by HCD on aggregation queries — do not pass `options.limit` to the CQL statement.

**CQL emitted:**
```cql
-- hcd_mykeyspace_products = SAI index name (auto-generated for collections)
SELECT * FROM mykeyspace.products
WHERE expr(hcd_mykeyspace_products,
  '{"query":{"match":{"description":"cassandra"}},
    "aggs":{"by_category":{"terms":{"field":"category.keyword","size":10}}}}')
```

**Todo list**
1. Create `SearchAggregateCommand` record in `api/model/command/impl` — fields: `JsonNode aggregations` (required), `JsonNode query` (optional).
2. Create `SearchAggregateCommandResolver` in `service/resolver` — validates OpenSearch enabled, builds `expr(...)` body from `aggregations` + optional `query`, dispatches to new `SearchAggregateOperation`.
3. Create `SearchAggregateOperation` in `service/operation` — emits `SELECT * FROM … WHERE expr(...)`, reads the one-row result, maps columns to `status.aggregations`.
4. Add `OPEN_SEARCH_NOT_ENABLED` guard (reuse existing code); add `OPEN_SEARCH_AGGREGATION_TOO_LARGE` error code mapped from HCD's `InvalidRequestException`.
5. Register the command in the command router for both collection-level and table-level namespaces.
6. Unit tests: aggs-only body, aggs + query body, missing OpenSearch index → error, oversized response → error.

**Relevant context**
- Command pattern: [`CountDocumentsCommand`](src/main/java/io/stargate/sgv2/jsonapi/api/model/command/impl/CountDocumentsCommand.java)
- Operation pattern: [`CountCollectionOperation`](src/main/java/io/stargate/sgv2/jsonapi/service/operation/collections/CountCollectionOperation.java)
- Error mapping: existing `OPEN_SEARCH_NOT_ENABLED` guard in `CollectionFilterResolver`

#### 4.6.6 Implementation Tips (clients)

> Client SDK documentation: [HCD API Reference — Instantiate Client](https://docs.datastax.com/en/hyper-converged-database/2.0/api-reference/instantiate-client.html) and sub-pages.

**Java — using `OpenSearchQueryBuilder` (recommended)**
```java
// Faceted search — category counts + price ranges
AggregationResult result = collection.searchAggregate(
    OpenSearchQuery.builder()
        .match("description", "cassandra database")
        .build(),
    OpenSearchAggregations.builder()
        .terms("by_category", "category.keyword", 10)
        .range("price_ranges", "price",
               Range.to(25), Range.between(25, 100), Range.from(100))
        .build()
);

result.getBuckets("by_category").forEach(b ->
    System.out.println(b.getKey() + ": " + b.getDocCount())
);

// Metrics — no query scope (aggregate all)
AggregationResult metrics = collection.searchAggregate(
    null,   // no query — aggregate all documents
    OpenSearchAggregations.builder()
        .sum("total_revenue", "amount")
        .avg("avg_order", "amount")
        .cardinality("unique_customers", "customer_id.keyword")
        .build()
);
double revenue = metrics.getMetricValue("total_revenue");
```

**Python — using the `OpenSearchQueryBuilder`**
```python
from astrapy.search import query as q, agg as a

# Faceted search
result = collection.search_aggregate(
    query=q.match("description", "cassandra database"),
    aggregations={
        "by_category": a.terms("category.keyword", size=10),
        "price_ranges": a.range("price", ranges=[
            {"to": 25}, {"from": 25, "to": 100}, {"from": 100}
        ])
    }
)

for bucket in result["by_category"]["buckets"]:
    print(f"{bucket['key']}: {bucket['doc_count']}")

# Metrics — aggregate all documents
metrics = collection.search_aggregate(
    aggregations={
        "total_revenue":    a.sum("amount"),
        "avg_order":        a.avg("amount"),
        "unique_customers": a.cardinality("customer_id.keyword")
    }
)
print(metrics["total_revenue"]["value"])
```

**TypeScript / Node.js — using `OpenSearchQueryBuilder`**
```typescript
import { openSearch } from "@datastax/astra-db-ts";
const { match, terms, range, sum, avg, cardinality } = openSearch;

// Faceted search
const result = await collection.searchAggregate({
  query: match("description", "cassandra database"),
  aggregations: {
    by_category: terms("category.keyword", { size: 10 }),
    price_ranges: range("price", [{ to: 25 }, { from: 25, to: 100 }, { from: 100 }])
  }
});

for (const bucket of result.by_category.buckets) {
  console.log(`${bucket.key}: ${bucket.doc_count}`);
}

// Metrics
const metrics = await collection.searchAggregate({
  aggregations: {
    total_revenue:    sum("amount"),
    avg_order:        avg("amount"),
    unique_customers: cardinality("customer_id.keyword")
  }
});
console.log(metrics.total_revenue.value);
```

---

### 4.7 `countDocuments` with `$openSearch` filter

#### 4.7.1 Goal

The existing `countDocuments` command is extended to accept a `$openSearch` filter. When `$openSearch` is present in the filter, the count is resolved by OpenSearch via `SELECT COUNT(*) FROM … WHERE expr(...)`. The result is the number of documents matching the OpenSearch query — not a Cassandra row count.

This is distinct from a standard `countDocuments` with a normal filter (which uses Cassandra's native counting). The two paths are independent — any filter that uses `$openSearch` is routed to OpenSearch; any other filter stays on the Cassandra path.

> **Tables:** `countDocuments` is currently **not supported for Tables** in the Data API. This section applies to **Collections only**. Table support for `countDocuments` (with or without `$openSearch`) is out of scope for this version.

`OPEN_SEARCH_NOT_ENABLED` is thrown if the collection has no OpenSearch index, regardless of whether the query would have matched documents.

#### 4.7.2 How it works

```mermaid
sequenceDiagram
    participant APP as Application
    participant API as Data API
    participant CQL as CQL / Cassandra
    participant OQH as OpenSearchQueryHandler
    participant OS as OpenSearch

    APP->>API: countDocuments<br/>filter: {$openSearch: {match: {name: Widget}}}
    API->>API: detect $openSearch key<br/>validate openSearch enabled<br/>build OpenSearchCollectionFilter
    API->>CQL: SELECT COUNT(*) FROM products<br/>WHERE expr(hcd_ks_products, {"match":{"name":"Widget"}})<br/>(hcd_ks_products = SAI index name)
    CQL->>OQH: intercept expr() for COUNT(*)<br/>resolves SAI index → OS index name
    OQH->>OS: POST /[os-index-name]/_count {query:{match:{name:Widget}}}
    OS-->>OQH: numFound count
    OQH-->>CQL: count value
    CQL-->>API: count row
    API-->>APP: {status:{count: 42}}
```

> Note: HCD returns the OpenSearch `numFound` as the count — not the Cassandra row count. For eventually-consistent indexes (recently written documents not yet replicated) the count may lag slightly.

#### 4.7.3 OpenSearch fields in the Data API Payload

The `filter.$openSearch` value is identical to its use in `find` — a verbatim OpenSearch Query DSL object. See §4.4.3 for the full field reference.

| Request field | Required | Description |
|---|---|---|
| `filter.$openSearch` | **yes** (for this path) | OpenSearch Query DSL. Must be the only key in the filter object. |

| Response field | Type | Description |
|---|---|---|
| `status.count` | Integer | Number of documents matching the OpenSearch query according to OpenSearch's `numFound`. |

> **`OPEN_SEARCH_NOT_ENABLED`**: thrown when the collection was not created with `openSearch.enabled: true` (collections) or has no `createOpenSearchIndex` (tables). Error message identifies the resource by name.

#### 4.7.4 Sample payloads

**Count documents matching a full-text query:**
```json
{
  "countDocuments": {
    "filter": {
      "$openSearch": {
        "match": { "description": "cassandra distributed database" }
      }
    }
  }
}
```
Response:
```json
{
  "status": { "count": 127 }
}
```

---

**Count with a boolean query:**
```json
{
  "countDocuments": {
    "filter": {
      "$openSearch": {
        "bool": {
          "must":   [{ "match": { "title": "cassandra" } }],
          "filter": [{ "term":  { "status": "published" } }]
        }
      }
    }
  }
}
```
Response:
```json
{
  "status": { "count": 43 }
}
```

---

**Count all OpenSearch-indexed documents (match_all):**
```json
{
  "countDocuments": {
    "filter": {
      "$openSearch": { "match_all": {} }
    }
  }
}
```

---

**Error — `$openSearch` on a collection without OpenSearch:**
```json
{
  "errors": [{
    "message": "The collection 'articles' does not have an OpenSearch index. Enable openSearch at collection creation time to use $openSearch with countDocuments.",
    "errorCode": "OPEN_SEARCH_NOT_ENABLED"
  }]
}
```

---

**Error — `$openSearch` combined with another filter:**
```json
{
  "countDocuments": {
    "filter": {
      "$openSearch": { "match": { "title": "tutorial" } },
      "status": "published"
    }
  }
}
```
Response:
```json
{
  "errors": [{
    "message": "$openSearch cannot be combined with other filter operators. The $openSearch filter must be the only condition in the filter object.",
    "errorCode": "INVALID_OPEN_SEARCH_FILTER"
  }]
}
```

#### 4.7.5 Implementation Tips (server)

**Behaviour rules**
- Detect `$openSearch` in the filter inside the `countDocuments` resolver, before the standard filter path — same early intercept pattern as in `CollectionFilterResolver`.
- On collections: validate `collectionSchemaObject.openSearchDef().enabled()`. If `false`, throw `OPEN_SEARCH_NOT_ENABLED` naming the collection.
- On tables: look up `ApiOpenSearchIndex`. If absent, throw `OPEN_SEARCH_NOT_ENABLED` naming the table.
- Build `OpenSearchCollectionFilter` (or `OpenSearchTableFilter`) from the `$openSearch` value — the **same filter class** used by the `find` command. Reuse without duplication.
- Emit `SELECT COUNT(*) FROM <table> WHERE expr(<saiIndexName>, '<dsl>')` instead of the normal count query, where `<saiIndexName>` is the **SAI index name** (auto-generated for collections; user-supplied for tables).
- The count result comes from `OpenSearchQueryHandler.numFound` (via the custom payload), not from a Cassandra row scan — the value is the OpenSearch hit count, not the Cassandra storage count.
- If `$openSearch` coexists with other filter keys → `INVALID_OPEN_SEARCH_FILTER` (same code as in `find`).

**Todo list**
1. Add `$openSearch` interception to `CountDocumentsCommandResolver` — detect before standard filter resolution (collections only; no table-equivalent needed for this version).
2. Reuse `OpenSearchCollectionFilter` — no new filter class needed.
3. Update `CountCollectionOperation` to emit `SELECT COUNT(*) WHERE expr(...)` when an `OpenSearchCollectionFilter` is detected.
4. The count value is returned via the standard CQL count row — HCD's `OpenSearchQueryHandler` intercepts `SELECT COUNT(*)` with an `expr(...)` predicate and returns the OpenSearch `numFound` value as a CQL long column, same as a normal Cassandra count. No special payload channel is required.
5. Extend error handling: `OPEN_SEARCH_NOT_ENABLED` (reuse existing), `INVALID_OPEN_SEARCH_FILTER` (reuse existing).

**Relevant context**
- Command: [`CountDocumentsCommand`](src/main/java/io/stargate/sgv2/jsonapi/api/model/command/impl/CountDocumentsCommand.java)
- Resolver: `CountDocumentsCommandResolver` (collections)
- Operation: [`CountCollectionOperation`](src/main/java/io/stargate/sgv2/jsonapi/service/operation/collections/CountCollectionOperation.java)
- Reuse filter: `OpenSearchCollectionFilter` (new, see §4.4.5)

#### 4.7.6 Implementation Tips (clients)

> Client SDK documentation: [HCD API Reference — Instantiate Client](https://docs.datastax.com/en/hyper-converged-database/2.0/api-reference/instantiate-client.html) and sub-pages.

**Java**
```java
// Count with a match query
long count = collection.countDocuments(
    Filters.search(
        OpenSearchQuery.builder()
            .match("description", "cassandra distributed database")
            .build()
    )
);

// Count with a boolean query
long publishedCount = collection.countDocuments(
    Filters.search(
        OpenSearchQuery.builder()
            .bool()
                .must(OpenSearchQuery.match("title", "cassandra"))
                .filter(OpenSearchQuery.term("status", "published"))
            .build()
    )
);
```

**Python**
```python
from astrapy.search import query as q

# Count with a match query
count = collection.count_documents(
    filter={"$openSearch": q.match("description", "cassandra distributed database")}
)

# Count with a boolean query
published_count = collection.count_documents(
    filter={"$openSearch": q.bool(
        must=[q.match("title", "cassandra")],
        filter=[q.term("status", "published")]
    )}
)
```

**TypeScript / Node.js**
```typescript
import { openSearch } from "@datastax/astra-db-ts";
const { match, bool, term } = openSearch;

// Count with a match query
const count = await collection.countDocuments({
  $openSearch: match("description", "cassandra distributed database")
});

// Count with a boolean query
const publishedCount = await collection.countDocuments({
  $openSearch: bool({
    must:   [match("title", "cassandra")],
    filter: [term("status", "published")]
  })
});
```

---

### 4.8 `deleteCollection` — also drops the OpenSearch index

#### 4.8.1 Goal

`deleteCollection` has an unchanged API signature. When the collection being deleted was created with `openSearch.enabled: true`, the implementation must drop the associated OpenSearch custom index **before** dropping the Cassandra table, otherwise the `DROP TABLE` succeeds but the orphaned Cassandra custom index (which depends on the table) would leave an inconsistent state. When `openSearch` was not enabled, the behaviour is unchanged.

> **Cassandra dependency order.** A Cassandra custom index depends on its backing table — you cannot drop the table while the index exists on some nodes (it will error or leave ghost metadata). The correct CQL sequence is always: `DROP INDEX` first, then `DROP TABLE`. For non-OpenSearch collections the Data API already does this for any SAI or other custom indexes present. For OpenSearch-enabled collections the Data API must additionally drop the OpenSearch custom index in the same pre-drop step.

> **HCD side-effect.** When HCD processes a `DROP INDEX` on an OpenSearch custom index, it also deletes the corresponding OpenSearch index from the OS cluster (the `indexName` stored in `WITH OPTIONS`). This is automatic and transparent — the Data API does not need to call the OpenSearch REST API directly. After `DROP INDEX` completes, the OS index no longer exists.

#### 4.8.2 How it works

```mermaid
sequenceDiagram
    participant APP as Application
    participant API as Data API
    participant CQL as Cassandra
    participant HCD as HCD Interceptor
    participant OS as OpenSearch

    APP->>API: deleteCollection name:articles
    API->>API: read CollectionSchemaObject\ncheck openSearchDef.enabled
    alt openSearch enabled
        API->>CQL: DROP INDEX IF EXISTS hcd_mykeyspace_articles
        CQL->>HCD: intercept DROP INDEX\ndelete OS index "articles-full-text"
        HCD->>OS: DELETE /articles-full-text
        OS-->>HCD: 200 OK
        CQL-->>API: OK
    end
    API->>CQL: DROP TABLE IF EXISTS mykeyspace.articles
    CQL-->>API: OK
    API-->>APP: {"status":{"ok":1}}
```

#### 4.8.3 Implementation Tips (server)

**Behaviour rules**
- Read `collectionSchemaObject.openSearchDef().enabled()` in `DeleteCollectionOperation` (or its equivalent task builder) before issuing DDL.
- If `enabled == true`: emit `DROP INDEX IF EXISTS <saiIndexName>` first, then `DROP TABLE IF EXISTS`. Both statements must complete successfully; use `IF EXISTS` on both so concurrent deletes are idempotent.
- If `enabled == false` (or `openSearchDef` absent — V1/V2 schema): proceed as today — drop the table directly. No change to existing behaviour.
- The SAI index name for collections is always `hcd_<keyspace>_<collectionName>` (auto-generated, stored in `collectionOpenSearchDef.saiIndexName`).
- Error during `DROP INDEX` (e.g. OS cluster unreachable): the Data API should surface this as a failure rather than silently skipping it and dropping the table. A partially-deleted state (table gone, index orphaned or vice versa) is worse than leaving both intact and letting the caller retry.

**CQL emitted**
```cql
-- Step 1: drop the OpenSearch custom index (triggers HCD to delete the OS index)
DROP INDEX IF EXISTS hcd_mykeyspace_articles;

-- Step 2: drop the collection table
DROP TABLE IF EXISTS mykeyspace.articles;
```

**Todo list**
1. In `DeleteCollectionOperation` (or `DropCollectionDBTask`), read `collectionSchemaObject.openSearchDef().enabled()`.
2. If `true`, prepend a `DROP INDEX IF EXISTS <saiIndexName>` CQL statement to the DDL sequence — before the `DROP TABLE`.
3. Confirm the SAI index name is reliably available from `collectionOpenSearchDef.saiIndexName()` at deletion time (it is persisted in the collection comment JSON at creation time).
4. Unit tests: V3 collection with `openSearch.enabled: true` → both DROP INDEX and DROP TABLE emitted in order; V2 collection (no `openSearch`) → only DROP TABLE emitted.

**Relevant context**
- Operation: [`DeleteCollectionOperation`](src/main/java/io/stargate/sgv2/jsonapi/service/operation/collections/DeleteCollectionOperation.java)
- Schema: `collectionSchemaObject.openSearchDef()` — available at deletion time via the schema read-back path

#### 4.8.4 Implementation Tips (clients)

The `deleteCollection` API surface is unchanged. Client code requires no modification.

**Java**
```java
// Unchanged — deleteCollection works the same regardless of whether OpenSearch is enabled.
// The Data API handles DROP INDEX internally before dropping the table.
database.deleteCollection("articles");
```

**Python**
```python
# Unchanged
database.drop_collection("articles")
```

**TypeScript / Node.js**
```typescript
// Unchanged
await db.dropCollection("articles");
```

---

### 5.3 `listIndexes` on Tables — includes OpenSearch indexes

#### 5.3.1 Goal

The existing `listIndexes` command (table-level only) must return OpenSearch indexes alongside the regular SAI, text, and vector indexes it already lists. No change to the request format is required — OpenSearch indexes appear automatically in the response as entries with `indexType: "openSearch"`.

`listIndexes` is **table-only**: it is not available on collections. Collections do not have user-facing indexes; their schema configuration is returned by `findCollections explain: true`.

After `createOpenSearchIndex` the new index appears in `listIndexes` with full details.
After `dropIndex` the entry is no longer present.

#### 5.3.2 Response shape

Each index entry in the `status.indexes` array has the same three-field envelope regardless of type:

| Field | Type | Description |
|---|---|---|
| `name` | `string` | The SAI index name — the Cassandra custom index identifier. For OpenSearch indexes this is the `name` value supplied to `createOpenSearchIndex`. |
| `indexType` | `string` | `"regular"`, `"text"`, `"vector"`, or `"openSearch"`. |
| `definition` | `object` | Type-specific definition. For `openSearch`: `columns` (list, omitted when all non-PK columns are covered) and `options` (`openSearchIndexName`, `numShards`, `numReplicas`, `mappings`, `applyDefaultSchema` — each omitted when absent or default). |

`definition.options.openSearchIndexName` is the **OS index name** (the OpenSearch-side index). It is omitted when it equals the HCD default (`hcd_<keyspace>_<tableName>`).

#### 5.3.3 Sample payloads

**Request:**
```json
{ "listIndexes": {} }
```

**Response — OpenSearch index listed alongside SAI indexes, with `explain: true`:**
```json
{
  "listIndexes": { "options": { "explain": true } }
}
```
```json
{
  "status": {
    "indexes": [
      {
        "name": "articles_title_sai",
        "indexType": "regular",
        "definition": { "column": "title" }
      },
      {
        "name": "articles_search",
        "indexType": "openSearch",
        "definition": {
          "columns": ["title", "body", "author"],
          "options": {
            "openSearchIndexName": "articles-full-text"
          }
        }
      }
    ]
  }
}
```

> `"name": "articles_search"` is the **SAI index name** (Cassandra identifier, used in `expr(articles_search, ...)`).
> `"definition.options.openSearchIndexName": "articles-full-text"` is the **OS index name** (the OpenSearch index).

**Response — minimal OpenSearch index (all non-PK columns, no explicit OS index name set):**
```json
{
  "status": {
    "indexes": [
      {
        "name": "users_search",
        "indexType": "openSearch",
        "definition": {}
      }
    ]
  }
}
```

> `definition` is an empty object when no columns were specified (all non-PK replicated) and no options were set.

**Response — `listIndexes` without `explain: true` (names only):**
```json
{
  "status": {
    "indexes": ["articles_title_sai", "articles_search"]
  }
}
```

#### 5.3.4 Implementation Tips (server)

**Behaviour rules**

- `listIndexes` already calls `schemaObject.apiTableDef().indexesIncludingUnsupported()` and iterates `allIndexes()`. `ApiOpenSearchIndex` is present in that container once `IndexFactoryFromCql` recognises `class_name = 'OpenSearchIndex'` (§6.5).
- `ApiOpenSearchIndex.getSchemaDescription(SchemaDescSource.DDL_SCHEMA_OBJECT)` returns an `IndexDesc<OpenSearchIndexDefinitionDesc>` with `indexType() = "openSearch"`. No additional serialisation work is needed.
- `ApiOpenSearchIndex.targetColumn()` returns `null` (multi-column index). `ApiIndexDefContainer.put()` guards `byTarget.put(...)` behind `!indexDef.isUnsupported()` — since `ApiOpenSearchIndex.isUnsupported()` returns `false`, the null key is written into `byTarget`. This is safe because `byTarget` is only queried by `allIndexesFor(CqlIdentifier)` and `firstIndexFor(CqlIdentifier)` — neither is called with a null argument during `listIndexes`. The `allIndexes()` path used by `ListIndexesDBTask` reads from `byName` only and is unaffected.
- The `definition.columns` field is omitted when the list is empty (all non-PK columns replicated). `@JsonInclude(NON_NULL)` on `OpenSearchIndexDefinitionDesc` handles this automatically.
- `definition.options.openSearchIndexName` is populated from the `indexName` `WITH OPTIONS` key stored in the CQL index metadata. It is omitted when null (i.e. the user did not set a custom OS index name).

**Todo list**

1. ✅ `IndexFactoryFromCql.isSupported()` — already returns `true` for `class_name = 'OpenSearchIndex'`.
2. ✅ `IndexFactoryFromCql.create()` — already dispatches to `ApiOpenSearchIndex.fromCqlMetadata()` for OpenSearch indexes.
3. ✅ `ApiOpenSearchIndex.getSchemaDescription()` — already implemented; returns `indexType: "openSearch"` with `OpenSearchIndexDefinitionDesc`.
4. ✅ `ListIndexesDBTask.getSchema()` — already calls `getSchemaDescription()` on all indexes including OpenSearch; no change required.
5. ✅ No serialisation changes needed — `OpenSearchIndexDefinitionDesc` is already a proper `IndexDefinitionDesc` and is serialised correctly by Jackson.

**Relevant context**
- Task: [`ListIndexesDBTask`](src/main/java/io/stargate/sgv2/jsonapi/service/operation/ListIndexesDBTask.java)
- Resolver: [`ListIndexesCommandResolver`](src/main/java/io/stargate/sgv2/jsonapi/service/resolver/ListIndexesCommandResolver.java)
- Schema model: [`ApiOpenSearchIndex`](src/main/java/io/stargate/sgv2/jsonapi/service/schema/tables/ApiOpenSearchIndex.java) — `getSchemaDescription()` returns the full definition
- Container: [`ApiIndexDefContainer`](src/main/java/io/stargate/sgv2/jsonapi/service/schema/tables/ApiIndexDefContainer.java) — `allIndexes()` used by the task
- Factory: [`IndexFactoryFromCql`](src/main/java/io/stargate/sgv2/jsonapi/service/schema/tables/factories/IndexFactoryFromCql.java) — dispatches to `ApiOpenSearchIndex.fromCqlMetadata()`

#### 5.3.5 Implementation Tips (clients)

The `listIndexes` API surface is unchanged. Client code requires no modification to receive OpenSearch index entries — they appear automatically in the response alongside other index types.

**Java**
```java
List<TableIndexInfo> indexes = database.getTable("articles")
    .listIndexes(ListIndexesOptions.builder().explain(true).build());

indexes.forEach(idx -> {
    System.out.println(idx.getName() + " (" + idx.getIndexType() + ")");
    if ("openSearch".equals(idx.getIndexType())) {
        OpenSearchIndexDefinition def = (OpenSearchIndexDefinition) idx.getDefinition();
        System.out.println("  OS index: " + def.getOptions().getOpenSearchIndexName());
        System.out.println("  Columns:  " + def.getColumns());
    }
});
```

**Python**
```python
indexes = table.list_indexes()
for idx in indexes:
    print(f"{idx.name} ({idx.index_type})")
    if idx.index_type == "openSearch":
        print(f"  OS index: {idx.definition.options.open_search_index_name}")
        print(f"  Columns:  {idx.definition.columns}")
```

**TypeScript / Node.js**
```typescript
const indexes = await table.listIndexes({ explain: true });
for (const idx of indexes) {
  console.log(`${idx.name} (${idx.indexType})`);
  if (idx.indexType === "openSearch") {
    console.log(`  OS index: ${idx.definition?.options?.openSearchIndexName}`);
    console.log(`  Columns:  ${idx.definition?.columns}`);
  }
}
```

---

### 5.4 `dropIndex` on Tables — drops the OpenSearch custom index

#### 5.4.1 Goal

The existing `dropIndex` command (which drops SAI, text, and vector indexes from tables) must also handle OpenSearch indexes created via `createOpenSearchIndex`. The API signature is unchanged — the caller passes the SAI index name (the Cassandra custom index identifier) and optionally `ifExists`. The implementation must recognise that the target is an OpenSearch-type index and route through the same drop path.

> **HCD side-effect.** As with `deleteCollection`, when HCD processes a `DROP INDEX` on an OpenSearch custom index it automatically deletes the corresponding OS index from the OpenSearch cluster. The Data API does not need to call OpenSearch directly.

> **Scope.** `dropIndex` drops only the index — not the table. After `dropIndex`, the table remains intact and writable; it simply no longer replicates to OpenSearch and `$openSearch` queries on it will return `OPEN_SEARCH_NOT_ENABLED`.

#### 5.4.2 How it works

```mermaid
sequenceDiagram
    participant APP as Application
    participant API as Data API
    participant CQL as Cassandra
    participant HCD as HCD Interceptor
    participant OS as OpenSearch

    APP->>API: dropIndex name:articles_search
    API->>API: look up ApiOpenSearchIndex\nby name in tableSchemaObject.indexes()
    API->>CQL: DROP INDEX IF EXISTS articles_search
    CQL->>HCD: intercept DROP INDEX\ndelete OS index "articles-full-text"
    HCD->>OS: DELETE /articles-full-text
    OS-->>HCD: 200 OK
    CQL-->>API: OK
    API-->>APP: {"status":{"ok":1}}
```

#### 5.4.3 Implementation Tips (server)

**Behaviour rules**
- `dropIndex` already issues `DROP INDEX [IF EXISTS] <name>`. The SAI index name for OpenSearch indexes is the user-supplied `name` from `createOpenSearchIndex`.
- The only required change is ensuring `IndexFactoryFromCql.isSupported()` recognises the `OPEN_SEARCH` class name (see §5.5) so that `ApiOpenSearchIndex` is resolved from schema metadata rather than falling back to `UnsupportedIndex`. Without this, the resolver cannot confirm the index type and may reject the operation.
- `IF EXISTS` handling follows the same pattern as other index types — map `options.ifNotExists` to `IF EXISTS` in the CQL statement.
- After the `DROP INDEX` completes, `listIndexes` must no longer return an entry for this index name. No extra cleanup step is required — Cassandra schema metadata is updated by the `DROP INDEX` itself.
- No validation against the OS cluster is required — HCD handles the OS-side deletion transparently.

**CQL emitted**
```cql
-- ifExists: true
DROP INDEX IF EXISTS articles_search;

-- ifExists: false (default) — errors if the index does not exist
DROP INDEX articles_search;
```

**Todo list**
1. Confirm `IndexFactoryFromCql.isSupported()` returns `true` for `class_name = 'OpenSearchIndex'` (§5.5 todo). If not, `dropIndex` will silently treat the index as `UnsupportedIndex` and may refuse the operation.
2. Verify `DropIndexCommandResolver` (or `DropIndexDBTask`) accepts `ApiIndexType.OPEN_SEARCH` indexes — no special-casing should be needed since the CQL `DROP INDEX` statement is type-agnostic, but confirm no guard blocks non-SAI types.
3. Integration test: create a table, call `createOpenSearchIndex`, call `dropIndex`, verify `listIndexes` returns empty and `$openSearch` returns `OPEN_SEARCH_NOT_ENABLED`.

**Relevant context**
- Command: `DropIndexCommand` (existing)
- Resolver: `DropIndexCommandResolver` (existing)
- Factory: [`IndexFactoryFromCql`](src/main/java/io/stargate/sgv2/jsonapi/service/schema/tables/factories/IndexFactoryFromCql.java) — must recognise `OpenSearchIndex` class name (see §5.5)

#### 5.4.4 Implementation Tips (clients)

The `dropIndex` API surface is unchanged. Client code requires no modification.

**Java**
```java
// Unchanged — same call regardless of index type
database.getTable("articles").dropIndex("articles_search");

// With ifExists guard
database.getTable("articles").dropIndex("articles_search",
    DropIndexOptions.builder().ifExists(true).build());
```

**Python**
```python
# Unchanged
table = database.get_table("articles")
table.drop_index("articles_search")

# With if_exists guard
table.drop_index("articles_search", if_exists=True)
```

**TypeScript / Node.js**
```typescript
// Unchanged
const table = db.table("articles");
await table.dropIndex("articles_search");

// With ifExists guard
await table.dropIndex("articles_search", { ifExists: true });
```

---

### 5.5 `searchAggregate` on Tables — aggregations and faceted search

#### 5.5.1 Goal

Tables that have a `createOpenSearchIndex` index (§5.1) gain the same `searchAggregate` command as collections (§4.6). The behaviour and request/response shape are identical — `aggregations` (required) + `query` (optional) → `status.aggregations` map. The only difference is the routing path: table targets use `SearchAggregateTableOperation` and look up the SAI index name from `ApiOpenSearchIndex` rather than `collectionOpenSearchDef`.

`OPEN_SEARCH_NOT_ENABLED_FOR_TABLE` is thrown if the table does not have an OpenSearch index.

#### 5.5.2 How it works

```mermaid
sequenceDiagram
    participant APP as Application
    participant API as Data API
    participant CQL as CQL / Cassandra
    participant OQH as OpenSearchQueryHandler
    participant OS as OpenSearch

    APP->>API: searchAggregate<br/>aggregations:{by_status:{terms:{field:status.keyword}}}<br/>query:{match:{body:cassandra}}
    API->>API: look up ApiOpenSearchIndex from tableSchemaObject<br/>resolve saiIndexName (= articles_search)<br/>build aggs+query body
    API->>CQL: SELECT * FROM articles<br/>WHERE expr(articles_search, {"query":...,"aggs":{...}})<br/>(articles_search = SAI index name)
    CQL->>OQH: intercept expr() — detects aggs key<br/>switch to aggregation mode
    OQH->>OS: POST /articles-ft/_search {query+aggs body}
    OS-->>OQH: aggregation result JSON
    OQH-->>CQL: 1 row, 1 text column per top-level agg
    CQL-->>API: aggregation row
    API->>API: deserialise each column as JSON<br/>build status.aggregations map
    API-->>APP: {status:{aggregations:{by_status:{buckets:[...]}}}}
```

#### 5.5.3 OpenSearch fields in the Data API Payload

The request and response fields are identical to the collection path — see [§4.6.3](#463-opensearch-fields-in-the-data-api-payload). The DSL is matched against the replicated **table column names** (not unpacked JSON document fields) so field names in `aggregations` and `query` must match the actual column names of the table.

#### 5.5.4 Sample payloads

**Category facets — count articles by status:**
```json
{
  "searchAggregate": {
    "query": {
      "match": { "body": "cassandra database" }
    },
    "aggregations": {
      "by_status": {
        "terms": { "field": "status.keyword", "size": 5 }
      }
    }
  }
}
```
Response:
```json
{
  "status": {
    "aggregations": {
      "by_status": {
        "buckets": [
          { "key": "published",  "doc_count": 38 },
          { "key": "draft",      "doc_count": 14 },
          { "key": "archived",   "doc_count": 6 }
        ]
      }
    }
  }
}
```

---

**Date histogram — articles per month:**
```json
{
  "searchAggregate": {
    "aggregations": {
      "articles_per_month": {
        "date_histogram": {
          "field": "published_at",
          "calendar_interval": "month"
        },
        "aggs": {
          "avg_word_count": { "avg": { "field": "word_count" } }
        }
      }
    }
  }
}
```
→ CQL emitted:
```cql
-- articles_search = SAI index name (user-supplied via createOpenSearchIndex.name)
SELECT * FROM mykeyspace.articles
WHERE expr(articles_search,
  '{"aggs":{"articles_per_month":{"date_histogram":{...},"aggs":{"avg_word_count":{"avg":{...}}}}}}')
```

---

**Error — `searchAggregate` on a table without OpenSearch:**
```json
{
  "errors": [{
    "message": "The '$openSearch' filter can only be used on tables that have an OpenSearch index. Use createOpenSearchIndex to create one before using '$openSearch' on table mykeyspace.articles.",
    "errorCode": "OPEN_SEARCH_NOT_ENABLED_FOR_TABLE"
  }]
}
```

#### 5.5.5 Implementation Tips (server)

**Behaviour rules**
- `searchAggregate` is registered at the table level via `SearchAggregateCommandResolver.resolveTableCommand()` — the same resolver that handles collection aggregations. The dispatch to the correct operation (`SearchAggregateCollectionOperation` vs `SearchAggregateTableOperation`) happens inside the resolver.
- `SearchAggregateTableOperation.of()` looks up `ApiOpenSearchIndex` from `tableSchemaObject.apiTableDef().indexes().allIndexes()` by type `OPEN_SEARCH`. If absent, throws `OPEN_SEARCH_NOT_ENABLED_FOR_TABLE`.
- The SAI index name is read from `ApiOpenSearchIndex.saiIndexName()`.
- The `expr()` body and result parsing are identical to the collection path — only the schema object type differs.
- `LIMIT` is not passed — HCD rejects `LIMIT` on aggregation queries.
- Error mapping: same `max_agg_json_bytes` / `max_agg_response_bytes` pattern as the collection path.

**CQL emitted:**
```cql
-- articles_search = SAI index name (user-supplied via createOpenSearchIndex.name)
SELECT * FROM mykeyspace.articles
WHERE expr(articles_search,
  '{"query":{"match":{"body":"cassandra database"}},
    "aggs":{"by_status":{"terms":{"field":"status.keyword","size":5}}}}')
```

**Implementation summary**
1. ✅ Created `SearchAggregateTableOperation` in `service/operation/tables` — mirrors `SearchAggregateCollectionOperation` but takes `CommandContext<TableSchemaObject>`, resolves `ApiOpenSearchIndex` from the table def, and uses `OPEN_SEARCH_NOT_ENABLED_FOR_TABLE` error code.
2. ✅ Added `resolveTableCommand()` to `SearchAggregateCommandResolver` — delegates to `SearchAggregateTableOperation.of()`.

**Relevant context**
- Collection parallel: [`SearchAggregateCollectionOperation`](src/main/java/io/stargate/sgv2/jsonapi/service/operation/collections/SearchAggregateCollectionOperation.java)
- Table operation: [`SearchAggregateTableOperation`](src/main/java/io/stargate/sgv2/jsonapi/service/operation/tables/SearchAggregateTableOperation.java) *(new)*
- Resolver: [`SearchAggregateCommandResolver`](src/main/java/io/stargate/sgv2/jsonapi/service/resolver/SearchAggregateCommandResolver.java)
- Index lookup: `tableSchemaObject.apiTableDef().indexes().allIndexes()` filtered by `ApiIndexType.OPEN_SEARCH`

#### 5.5.6 Implementation Tips (clients)

The `searchAggregate` API surface is the same as for collections — see [§4.6.6](#466-implementation-tips-clients) for full Java, Python, and TypeScript examples. The only difference is that field names in `aggregations` and `query` must match **table column names** (e.g. `published_at` instead of a document property name). The client SDK calls the command on a `Table` object instead of a `Collection` object.

---


## 6. Cross-Cutting Concerns

### 6.1 Class hierarchy

> **Diagram 7 — New classes in context.** Shows where each new class sits inside the existing Data API type hierarchy.

```mermaid
classDiagram
    direction TB

    class CreateCollectionCommand {
        +String name
        +Options options
    }
    class Options {
        +VectorSearchDesc vector
        +IndexingDesc indexing
        +LexicalDesc lexical
        +RerankDesc rerank
        +OpenSearchDesc openSearch
    }
    class OpenSearchDesc {
        +Boolean enabled
        +Integer numShards
        +Integer numReplicas
        +String indexName
        +JsonNode mappings
    }
    class CollectionOpenSearchDef {
        +boolean enabled
        +int numShards
        +int numReplicas
        +String indexName
        +JsonNode mappings
        +fromApiDesc() SchemaHolder
        +toApiDesc() OpenSearchDesc
        +SCHEMA_DEFAULTS SchemaDefaults
    }
    class CollectionSchemaObject {
        +SchemaHolder~CollectionLexicalDef~ lexicalDef
        +SchemaHolder~CollectionRerankDef~ rerankDef
        +SchemaHolder~CollectionOpenSearchDef~ openSearchDef
    }

    CreateCollectionCommand *-- Options
    Options *-- OpenSearchDesc
    OpenSearchDesc ..> CollectionOpenSearchDef : resolved by resolver
    CollectionOpenSearchDef --* CollectionSchemaObject

    class ApiIndexDef {
        <<abstract>>
        +ApiIndexType indexType
        +CqlIdentifier saiIndexName
        +getSchemaDescription()
    }
    class ApiRegularIndex
    class ApiTextIndex
    class ApiVectorIndex
    class ApiOpenSearchIndex {
        +FROM_DESC_FACTORY UserDescFactory
        +FROM_CQL_FACTORY CqlTypeFactory
    }

    ApiIndexDef <|-- ApiRegularIndex
    ApiIndexDef <|-- ApiTextIndex
    ApiIndexDef <|-- ApiVectorIndex
    ApiIndexDef <|-- ApiOpenSearchIndex

    class CollectionFilter {
        <<abstract>>
        +get() BuiltCondition
    }
    class MatchCollectionFilter {
        note: existing — dollar-match lexical
    }
    class OpenSearchCollectionFilter {
        +String saiIndexName
        +JsonNode queryDsl
        note: new — dollar-search on collections
    }
    class OpenSearchTableFilter {
        +String saiIndexName
        +JsonNode queryDsl
        note: new — dollar-search on tables
    }

    CollectionFilter <|-- MatchCollectionFilter
    CollectionFilter <|-- OpenSearchCollectionFilter

    class TableFilter {
        <<abstract>>
    }
    TableFilter <|-- OpenSearchTableFilter

    class SearchAggregateCommand {
        +JsonNode aggregations
        +JsonNode query
    }
    class SearchAggregateOperation {
        +execute() AggregationResult
    }
    SearchAggregateCommand ..> SearchAggregateOperation : dispatched by resolver

    class ApiIndexType {
        <<enumeration>>
        REGULAR
        TEXT
        VECTOR
        OPEN_SEARCH
    }
    ApiOpenSearchIndex --> ApiIndexType
```

---

### 6.2 Schema version evolution

> **Diagram 8 — CollectionSchemaVersion chain.** Shows how the new V_3 extends the existing version ladder and how old collections are handled on read.

```mermaid
flowchart LR
    subgraph VERSIONS["CollectionSchemaVersion — version ladder"]
        V0["V_0\npre-release\nno lexical\nno rerank\nno openSearch"]
        V1["V_1\nlexical added"]
        V2["V_2\nlexical + rerank"]
        V3["V_3\nlexical + rerank\n+ openSearch"]
        V0 -->|"CollectionSettingsV1Reader"| V1
        V1 -->|"CollectionSettingsV2Reader"| V2
        V2 -->|"CollectionSettingsV3Reader"| V3
    end

    subgraph COMPAT["Backward compatibility on read"]
        OLD["Existing collection\nschema version V_1 or V_2\nopenSearch key absent from comment JSON"]
        READ["V1Reader / V2Reader\nreads openSearch key"]
        DEF["Key absent → SchemaDefaults.forPreRelease()\nopenSearchDef.enabled = false\nno $openSearch allowed"]
        OLD --> READ --> DEF
    end
```

---

### 6.3 Error codes
New `SchemaException.Code` (or `FilterException.Code`) values to add:

| Code | When thrown |
|------|-------------|
| `OPEN_SEARCH_NOT_ENABLED` | Any OpenSearch command (`$openSearch`, `searchAggregate`, `countDocuments` with `$openSearch`) used on a collection/table that has no OpenSearch index configured. Applies to both collections (no `openSearch.enabled: true` at creation) and tables (no `createOpenSearchIndex` run). |
| `INVALID_OPEN_SEARCH_FILTER` | `$openSearch` combined with any other filter operator in the same filter object, or `options.limit` set alongside a `"size"` key in the `$openSearch` DSL (HCD rejects a CQL `LIMIT` combined with a DSL `size`). |
| `OPEN_SEARCH_INCOMPATIBLE_WITH_DENY_LIST` | `openSearch.enabled: true` + `indexing.deny` at collection creation. |
| `OPEN_SEARCH_INDEX_ALREADY_EXISTS` | Attempting to create a second OpenSearch index on the same table. |
| `OPEN_SEARCH_AGGREGATION_TOO_LARGE` | An aggregation response from OpenSearch exceeds the `max_agg_json_bytes` or `max_agg_response_bytes` bounds imposed by `OpenSearchQueryHandler`. Mirrors the HCD-level size guard. |
| `OPEN_SEARCH_COMMAND_NOT_SUPPORTED` | A `searchAggregate` or `countDocuments` with `$openSearch` is attempted on a resource type where the command is not supported (e.g. `searchAggregate` on a single-document endpoint). Reserved for future constraint enforcement. |
| `OPEN_SEARCH_CORRUPT_SCHEMA` | `CollectionSettingsV3Reader` finds a V3 collection with `openSearch.enabled: true` but a null `indexName` in the stored comment. This indicates the comment was written incorrectly at creation time. Thrown instead of silently recomputing a default, which could return wrong information and later cause `expr(...)` queries against a non-existent SAI index. |
| `OPEN_SEARCH_MISSING_FIELD_MAPPINGS` | `createCollection` with `openSearch.enabled: true` is missing `mappings`, or `mappings` is empty, or `_id` appears as a key in `mappings`. There is no auto-inference path for collections. |
| `RERANK_NOT_SUPPORTED_WITH_OPEN_SEARCH` | `findAndRerank` called with a `$openSearch` filter. The rerank pipeline does not support OpenSearch-backed result sets — block at `FindAndRerankCommandResolver` before `FindAndRerankOperationBuilder` constructs the inner `FindCommand`. |

### 6.4 `CollectionSchemaVersion`
The new `openSearch` field in the collection comment JSON requires schema version **V_3**. The existing V1/V2 readers must handle the absent `openSearch` key gracefully by returning `SchemaDefaults.forPreRelease()` (maps to `enabled: false`).

### 6.5 `isSupported()` check in `IndexFactoryFromCql`
The current `IndexFactoryFromCql.isSupported()` method only accepts SAI indexes (`CQLSAIIndex.isSAIIndex()`). OpenSearch indexes have `class_name = 'OpenSearchIndex'` which will fail that check. The method must be updated to also return `true` for the OpenSearch class name, otherwise existing OpenSearch indexes on a table will be silently represented as `UnsupportedIndex` when the schema is read.

### 6.6 HCD feature flag
An operator-level feature flag should gate these features (same pattern as `lexical` / `rerank`). Check whether a `FeatureFlagResolver` or equivalent already exists before introducing a new one.

### 6.7 Testing
Each sub-task should include:
- Unit tests for the new `*Def` / `*Filter` / `*Index` classes.
- Integration tests using `@QuarkusTest` for the command resolver → operation → CQL generation path.
- Schema round-trip tests: create collection with `openSearch`, read back via `findCollections`, verify serialised options match.
- Negative tests: `$openSearch` + other filters, `$openSearch` with no index, deny-list + openSearch.

**Additional test cases for `searchAggregate`:**
- `terms` aggregation scoped by a `match` query — verify `status.aggregations.*.buckets` shape.
- `date_histogram` with nested `avg` — verify bucket key format and nested aggregation value.
- Multiple metric aggregations (`sum`, `avg`, `cardinality`) — verify all values present in response map.
- `searchAggregate` on collection without `openSearch.enabled: true` → `OPEN_SEARCH_NOT_ENABLED`.
- `searchAggregate` on table without `createOpenSearchIndex` → `OPEN_SEARCH_NOT_ENABLED`.
- Oversized aggregation response mock → `OPEN_SEARCH_AGGREGATION_TOO_LARGE`.
- Aggregation keys are returned verbatim (not lowercased or transformed).

**Additional test cases for `countDocuments` with `$openSearch`:**
- `countDocuments` with `match` query on collection — verify `status.count` equals OpenSearch `numFound`.
- `countDocuments` with `bool` query (must + filter) — verify correct count returned.
- `countDocuments` with `match_all` — verify count equals total indexed document count.
- `countDocuments` with `$openSearch` + another filter key → `INVALID_OPEN_SEARCH_FILTER`.
- `countDocuments` with `$openSearch` on collection without OpenSearch → `OPEN_SEARCH_NOT_ENABLED`.
- `countDocuments` with `$openSearch` on table without OpenSearch index → `OPEN_SEARCH_NOT_ENABLED`.
- Verify `OpenSearchCollectionFilter` / `OpenSearchTableFilter` are shared between `find` and `countDocuments` — no duplicated filter class.

**Special property name testing matrix (Dots, Ampersands, Dollar Signs, EJSON):**
- **Ampersand & symbols in property names (`&`, `-`, `_`, `@`):**
  - Create collection with mappings containing `"r&d"` (`keyword`), `"author&co"` (`text`), `"tag-name"` (`keyword`), `"contact@email"` (`keyword`).
  - Insert documents containing fields `{"r&d": "AI Lab", "author&co": "Smith & Jones", "tag-name": "v1-alpha", "contact@email": "dev@company.com"}`.
  - Verify exact match queries with `term` on `r&d` and `tag-name`.
  - Verify text match query with `match` on `author&co`.
  - Verify `terms` aggregation on `tag-name` returns correct bucket counts.
- **EJSON Date fields (`$date`):**
  - Create collection with mapping `"eventDate": {"properties": {"$date": {"type": "date"}}}`.
  - Insert documents with `{"eventDate": {"$date": 1730419200000}}`.
  - Test `$openSearch` query with `range` filter on `eventDate.$date`.
  - Test `searchAggregate` with `date_histogram` on `eventDate.$date`.
  - Test sorting inside `$openSearch` DSL on `eventDate.$date`.
- **EJSON UUID & Binary fields (`$uuid`, `$binary`, `$objectId`):**
  - Create collection with mappings:
    - `"userId": {"properties": {"$uuid": {"type": "keyword"}}}`
    - `"avatar": {"properties": {"$binary": {"type": "binary"}}}`
    - `"refId": {"properties": {"$objectId": {"type": "keyword"}}}`
  - Insert documents with `{"userId": {"$uuid": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}, "refId": {"$objectId": "507f1f77bcf86cd799439011"}}`.
  - Test `$openSearch` query with `term` on `userId.$uuid` and `refId.$objectId`.
- **Nested objects and dot navigation:**
  - Create collection with mapping `"location": {"properties": {"city": {"type": "keyword"}, "zip": {"type": "keyword"}}}`.
  - Insert documents with `{"location": {"city": "Paris", "zip": "75001"}}`.
  - Test `$openSearch` query filtering on `location.city`.
  - Test `searchAggregate` grouping on `location.city`.

---

### 6.8 Write consistency and read-after-write guarantees

This section documents the consistency model between Cassandra writes and OpenSearch query results. Understanding these guarantees is essential for correct client behaviour and for writing reliable integration tests.

#### Eventual consistency — not read-my-writes

**OpenSearch replication is eventually consistent.** There is always a lag between the moment a document is written to Cassandra and the moment it becomes visible in OpenSearch `$openSearch` or `searchAggregate` results. Clients must never assume that a write is immediately queryable via OpenSearch.

The pipeline has two independent delay sources:

| Stage | Latency | Controlled by |
|---|---|---|
| **HCD replication** | Typically < 1 s | `BulkProcessor` batches up to 1 000 docs, flushes every 1 s |
| **OpenSearch refresh** | Typically 1 s for active indexes | OpenSearch's own refresh cycle (see below) |

End-to-end, a freshly inserted document should normally be searchable within **1–2 seconds** under typical load, but this is not guaranteed and can be higher under index pressure.

> **Do not use `$openSearch` results as a read-my-writes consistency check.** To confirm a write landed in Cassandra, use a normal `find` with a regular filter — not `$openSearch`. OpenSearch is a search replica, not the source of truth.

#### LWT (Lightweight Transaction) support

HCD intercepts **all** write path operations, including Lightweight Transactions (Paxos v1 and Paxos v2). The guarantee is:

- LWT outcomes are **eventually** reflected in OpenSearch — not immediately.
- If an LWT is recovered later (e.g. by a SERIAL read or another LWT that triggers Paxos reconciliation), HCD's `Replicator` intercepts the reconciliation and re-applies the correct value to OpenSearch.
- There is no direct LWT equivalent in OpenSearch. The OpenSearch replica is updated after the Cassandra Paxos round completes and HCD processes the write.

> **LWTs do not provide read-my-writes on the OpenSearch side.** After a conditional `insertOne` or `findOneAndUpdate` with a `$exists` guard, the result is visible in Cassandra immediately (at the agreed consistency level) but will only appear in OpenSearch after the replication + refresh cycle.

#### OpenSearch refresh interval

OpenSearch moves indexed documents into searchable segments via a **refresh** operation. By default:

- Indexes that have received at least one search request in the past 30 seconds are refreshed **every 1 second**.
- Indexes with no recent search activity are not refreshed automatically (to reduce overhead).

This means a document written to an active index is typically searchable within **~1 second**, but a quiet index may not expose a write until the next explicit or triggered refresh.

For reference: [OpenSearch — Optimize Refresh Interval](https://opensearch.org/blog/optimize-refresh-interval/)

> **PO decision — `refresh` parameter not exposed on `find`.** A `refresh` query option that forces an OpenSearch index refresh before executing the search was considered. Decision: **do not expose it.** Forcing a refresh on every read is a heavy operation that defeats the purpose of OpenSearch's buffered indexing model and would significantly degrade throughput for all other readers of the same index. Applications that need read-my-writes semantics should instead use the normal Cassandra `find` path (not `$openSearch`) for confirming writes, and accept the ~1–2 s eventual visibility window for OpenSearch queries.

#### HCD query path performance improvements (internal — no Data API surface impact)

The HCD `OpenSearchQueryHandler` received several performance improvements that benefit all `$openSearch`, `searchAggregate`, and `countDocuments` queries transparently:

- **Batched Cassandra reads:** hits returned by OpenSearch are now resolved in configurable-size batches (default 100, tunable via `cassandra.opensearch.lookup.batch_size`) submitted as a `SinglePartitionReadCommand.Group`, so all replica round-trips in a batch are fanned out concurrently. Previous behaviour was one read per hit, serialised. This is the main latency improvement for large result sets.
- **`source_only` path (tables only):** a `source_only: true` option in the `expr(...)` envelope bypasses Cassandra reads entirely and builds results from OpenSearch document source. Not useful for collections because `doc_json` is never replicated to OpenSearch — see §4.4.5 paging notes.
- **Live config reload:** `lookupBatchSize` and other config values can be reloaded via `reloadConfiguration` without a node restart.

None of these changes affect the Data API wire format or client behaviour.

#### Integration test pattern

Integration tests that write a document and immediately assert it is visible via `$openSearch` **must** call the OpenSearch Refresh API between the write and the read, otherwise the test will flake:

```http
POST /<index-name>/_refresh
```

In the HCD integration test suite this is done via a helper that calls the Refresh API before any assertion on `$openSearch` or `searchAggregate` results. **Never rely on `Thread.sleep()` as a substitute** — refresh timing is not deterministic.

```java
// Force refresh before asserting search results
openSearchClient.indices().refresh(r -> r.index(indexName));

// Now safe to assert
List<Document> results = collection.find(
    Filter.search(match("firstName", "Alice"))
).into(new ArrayList<>());
assertThat(results).hasSize(1);
```

---

## 7. Appendix — `OpenSearchQueryBuilder` API Reference

The `OpenSearchQueryBuilder` is a fluent builder available in all three SDK clients that produces OpenSearch Query DSL objects for use with `$openSearch`, `searchAggregate`, and `countDocuments`. It is the recommended alternative to constructing raw `Map` objects.

### 7.1 Query builders

| Builder method | DSL produced | Notes |
|---|---|---|
| `match(field, text)` | `{"match": {field: text}}` | Full-text match with field analysis |
| `match(field, text, options)` | `{"match": {field: {query: text, ...options}}}` | Extra options: `operator`, `fuzziness`, `boost` |
| `multiMatch(text, fields...)` | `{"multi_match": {query: text, fields: [...]}}` | Cross-field full-text search |
| `matchPhrase(field, phrase)` | `{"match_phrase": {field: phrase}}` | Exact phrase match |
| `term(field, value)` | `{"term": {field: value}}` | Exact keyword match (no analysis) |
| `terms(field, value...)` | `{"terms": {field: [...]}}` | Match any of the given values |
| `range(field, options)` | `{"range": {field: options}}` | Numeric / date range with `gte`, `lte`, `gt`, `lt` |
| `wildcard(field, pattern)` | `{"wildcard": {field: pattern}}` | Glob pattern, e.g. `"data*"` |
| `fuzzy(field, value)` | `{"fuzzy": {field: {value: value}}}` | Approximate match |
| `fuzzy(field, value, fuzziness)` | `{"fuzzy": {field: {value: value, fuzziness: fuzziness}}}` | Explicit edit distance |
| `matchAll()` | `{"match_all": {}}` | Returns all indexed documents |
| `bool()` | starts a `BoolQueryBuilder` | Compound queries — see below |
| `knn(field, vector, k)` | `{"knn": {field: {vector: [...], k: k}}}` | OpenSearch k-NN (advanced; requires `knn_vector` mapping) |

**`BoolQueryBuilder` methods** (chained off `bool()`):

| Method | DSL key |
|---|---|
| `.must(query...)` | `"must": [...]` — all clauses must match (score-affecting) |
| `.should(query...)` | `"should": [...]` — at least one should match |
| `.filter(query...)` | `"filter": [...]` — must match (no score contribution) |
| `.mustNot(query...)` | `"must_not": [...]` — must not match |
| `.minimumShouldMatch(n)` | `"minimum_should_match": n` |

**`OpenSearchQuery.builder()` methods** — wraps a query in a full search request body:

| Method | Effect |
|---|---|
| `.query(queryClause)` | Sets the `"query"` field |
| `.from(offset)` | Sets `"from"` for offset paging |
| `.size(n)` | Sets `"size"` (number of hits to return) |
| `.sort(sort...)` | Sets `"sort"` array — uses `Sort.by(field, order)` or `Sort.byScore(order)` |
| `.searchAfter(values...)` | Sets `"search_after"` for cursor-based deep paging |
| `.build()` | Returns the complete DSL object |

### 7.2 Aggregation builders

Used with `searchAggregate`. Available via `OpenSearchAggregations.builder()` (Java) / `agg.*` (Python) / named imports (TypeScript).

| Builder method | DSL produced | Notes |
|---|---|---|
| `terms(name, field, size)` | `{name: {"terms": {field, size}}}` | Bucket by distinct values. `size` caps result count. |
| `dateHistogram(name, field, interval)` | `{name: {"date_histogram": {field, calendar_interval}}}` | Date bucketing. `interval`: `"day"`, `"month"`, `"year"`, etc. |
| `range(name, field, ranges...)` | `{name: {"range": {field, ranges}}}` | Numeric/date range buckets. Use `Range.to(n)`, `Range.between(a,b)`, `Range.from(n)`. |
| `avg(name, field)` | `{name: {"avg": {field}}}` | Average of numeric field |
| `sum(name, field)` | `{name: {"sum": {field}}}` | Sum of numeric field |
| `min(name, field)` | `{name: {"min": {field}}}` | Minimum value |
| `max(name, field)` | `{name: {"max": {field}}}` | Maximum value |
| `cardinality(name, field)` | `{name: {"cardinality": {field}}}` | Distinct value count (HyperLogLog approximation) |
| `topHits(name, size, sort?)` | `{name: {"top_hits": {size, sort}}}` | Return top `size` matching documents per bucket |
| `nested(name, path, aggs)` | `{name: {"nested": {path}, "aggs": {...}}}` | Aggregate over nested objects |

### 7.3 Sort builders

Used inside `OpenSearchQuery.builder().sort(...)` for `search_after` paging.

| Method | DSL produced |
|---|---|
| `Sort.byScore("desc")` | `{"_score": {"order": "desc"}}` |
| `Sort.by("field", "asc")` | `{"field": {"order": "asc"}}` |
| `Sort.by("field", "desc")` | `{"field": {"order": "desc"}}` |

> **Tiebreaker requirement:** always include a unique field (e.g. document `_id` for collections, or a PK column for tables) as the last sort entry when using `search_after`. This guarantees no duplicate results across pages even when two documents have equal scores.

### 7.4 Cross-language usage summary

| Language | Import / namespace | Query builder entry point | Aggregation entry point |
|---|---|---|---|
| **Java** | `import static …OpenSearchQuery.*` | `OpenSearchQuery.builder()` / `match(…)` etc. | `OpenSearchAggregations.builder()` |
| **Python** | `from astrapy.search import query as q, agg as a` | `q.match(…)`, `q.bool(…)`, etc. | `a.terms(…)`, `a.avg(…)`, etc. |
| **TypeScript** | `import { openSearch } from "@datastax/astra-db-ts"` | `openSearch.match(…)`, `openSearch.bool(…)`, etc. | `openSearch.terms(…)`, `openSearch.avg(…)`, etc. |

