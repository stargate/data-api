# OpenSearch Collection Search Path Implementation Plan & Architecture Guide

## 1. Executive Summary & Objective

This document outlines the architecture, current state, implementation steps, and testing strategy for implementing the **OpenSearch collection search path** (`$search` operator with `find`, `findOne`, write commands, and `countDocuments`) in the Data API according to **Chapter 4.4 of the HCD OpenSearch Specification**.

### Goal
Allow Data API clients to execute full-text search and arbitrary OpenSearch Query DSL queries on OpenSearch-enabled collections:
```json
{
  "find": {
    "filter": {
      "$search": {
        "match": { "description": "cassandra distributed database" }
      }
    },
    "options": {
      "limit": 20
    }
  }
}
```

The Data API translates this into Cassandra's custom index expression:
```cql
SELECT key, tx_id, doc_json FROM <keyspace>.<collection>
WHERE expr(hcd_<keyspace>_<collection>, '{"match":{"description":"cassandra distributed database"}}')
```

HCD's `OpenSearchQueryHandler` intercepts the `expr(...)` predicate, routes the search DSL to OpenSearch, fetches matching Cassandra primary keys, and returns the corresponding rows to the Data API.

---

## 2. Architecture & Design Principles

### 2.1 Index Names Distinction
- **SAI Custom Index Name (Cassandra Identifier):** Used in `expr(<saiIndexName>, ...)` predicates. For collections, this is auto-generated as `hcd_<keyspace>_<collectionName>` (stored in `CollectionOpenSearchDef.saiIndexName()`).
- **OS Index Name (OpenSearch Identifier):** The index in the OpenSearch cluster (stored in `CollectionOpenSearchDef.indexName()`).

### 2.2 Exclusivity of `$search`
- `expr(...)` takes complete ownership of row fetching inside HCD's `OpenSearchQueryHandler`.
- It **cannot be combined with other CQL predicates** (such as regular column filters or other special operators like `$vector` / `$lexical`).
- Therefore, `$search` **must be the sole key** in the `filter` object. Combining `$search` with any other filter key must be rejected at filter validation time with `FilterException.Code.FILTER_INVALID_EXPRESSION` (or `INVALID_OPEN_SEARCH_FILTER`).

### 2.3 Arbitrary Query DSL Pass-Through
- The `$search` filter value is an **arbitrary JSON object** (e.g., `match`, `multi_match`, `match_phrase`, `bool`, `range`, `term`, `wildcard`, `fuzzy`, `match_all`, `from`/`size`, `sort`, `search_after`).
- The Data API does not inspect or transform the DSL (except for basic structural validation and checking that OpenSearch is enabled on the collection). It is passed verbatim into `expr(<saiIndexName>, '<dslJson>')`.

### 2.4 Paging and Sorting Support
1. **Cassandra `pageState` Paging:** Standard CQL cursor-based paging over `WHERE expr(...)`.
2. **DSL Paging (`from`/`size` or `search_after`):** Evaluated inside OpenSearch before returning matching PKs to Cassandra.
   - *Constraint:* If the DSL contains `"size"`, setting `options.limit` in the Data API command is disallowed.
3. **In-Memory Sorting:** If a Data API `sort` clause is provided alongside `$search`, `FindCollectionOperation.sorted(...)` sorts the fetched documents in-memory.
4. **Reranking Incompatibility:** `findAndRerank` does not support `$search`. `FindAndRerankCommandResolver` must reject `$search` before delegating to the rerank operation builder.

---

## 3. Current State Analysis

| Component | Status | Details |
|---|---|---|
| `CollectionOpenSearchDef.java` | ✅ Complete | Holds `enabled`, `indexName`, `numShards`, `numReplicas`, `mappings`, `saiIndexName`. |
| `CollectionOpenSearchDefSchemaFactory.java` | ✅ Complete | Schema factory registered for version `V_3`. |
| `CreateCollectionOperation.java` | ✅ Complete | Emits `CREATE CUSTOM INDEX ... USING 'OpenSearchIndex'`. |
| `DeleteCollectionCollectionOperation.java` | ✅ Complete | Drops custom index before table drop. |
| `BuiltConditionPredicate.java` | ✅ Complete | Added `EXPR("")` predicate for building `expr(<saiIndexName>, ?)`. |
| `OpenSearchIdEncoder.java` | ✅ Complete | Encodes Data API `_id` into HCD composite key `{_0=1, _1=<id>}`. |
| `OpenSearchDslIdRewriter.java` | ✅ Complete | Rewrites `term._id`, `terms._id`, and `ids.values` in DSL recursively; skips `search_after`. |
| `OpenSearchCollectionFilter.java` | ✅ Complete | Implements `CollectionFilter`, tags index usage, applies `_id` rewriting, emits `expr(<saiIndexName>, ?)`. |
| `CollectionFilterClauseBuilder.java` | ✅ Complete | Validates `$search` exclusivity and checks `openSearchDef.enabled()`. |
| `CollectionFilterResolver.java` | ✅ Complete | Dispatches `$search` captures to `OpenSearchCollectionFilter`. |
| `SearchCollectionFilter.java` | ✅ Removed | Obsolete placeholder removed. |
| `FindAndRerankCommandResolver.java` | ✅ Complete | Guards against `$search` combined with reranking. |
| `TestConstants.java` | ✅ Complete | Added `OPEN_SEARCH_COLLECTION_SCHEMA_OBJECT` helper. |

---

## 4. Implementation Steps

1. **Update `CollectionFilterClauseBuilder.java`**:
   - In `$search` handling:
     - Verify `schema.openSearchDef().enabled()` is `true`; throw `SchemaException.Code.OPEN_SEARCH_NOT_ENABLED_FOR_COLLECTION` if `false`.
     - Allow JSON Object DSL and validate exclusivity (only key in filter).
2. **Update `OpenSearchCollectionFilter.java`**:
   - Inherits `CollectionFilter`.
   - Stores `saiIndexName` and `queryDsl` (`JsonNode`).
   - Implements `get()` to emit `BuiltCondition.of(lhs, BuiltConditionPredicate.EXPR, new JsonTerm(queryDsl.toString()))`.
   - Sets `this.collectionIndexUsage.openSearchIndexTag = true;`.
3. **Update `CollectionFilterResolver.java`**:
   - Update rule / intercept for `$search` to create `OpenSearchCollectionFilter` with `schemaObject.openSearchDef().saiIndexName()` and `queryDsl`.
   - Delete obsolete `SearchCollectionFilter.java`.
4. **Update `FindAndRerankCommandResolver.java`**:
   - Check if `command.filterDefinition().json()` contains `$search` and throw `UNSUPPORTED_RERANKING_COMMAND`.
5. **Update `TestConstants.java` and Test Suites**:
   - Add `OPEN_SEARCH_COLLECTION_SCHEMA_OBJECT` in `TestConstants.java`.
   - Add unit tests in `FilterClauseBuilderTest` and `FindCommandResolverTest`.
   - Verify build and tests pass.
