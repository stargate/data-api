# Why `$search` (OpenSearch) and `$lexical` / `$match` (SAI/BM25) Must Stay Separate

## The question

Could OpenSearch be used as the back-end for the `$lexical` block — i.e. replace `$match` with a dedicated OpenSearch operator — instead of having two completely distinct query paths?

The short answer is **no**. This document explains the architectural reasons why, and why keeping them as two independent operators (`$match` for SAI-BM25, `$search` for OpenSearch) is the correct design.

---

## 1. Fundamentally different consistency contracts

| | SAI / `$lexical` + `$match` | OpenSearch / `$search` |
|---|---|---|
| **Write → visible** | Synchronous. A document inserted in one request is immediately filterable by `$match` in the next. | Asynchronous. HCD buffers writes in a WAL and flushes them to OpenSearch via `BulkProcessor` (up to 1 000 docs, every ~1 second). A freshly written document may not appear in `$search` results for up to a second. |
| **Failure model** | If Cassandra acknowledges the write, the index is updated. | If the OpenSearch cluster is down or slow, Cassandra still acknowledges the write but the document is silently absent from `$search` results until replication catches up. |

`$lexical` is explicitly designed to follow the "write then filter" pattern — the same contract as every other Data API operator. Routing `$match` through OpenSearch would silently weaken that contract for users who did not opt into the OpenSearch path.

---

## 2. Infrastructure requirements are incompatible with "enabled by default"

`$lexical` is turned **on by default** for all new collections. It works on any HCD instance with zero operator configuration — the SAI index is created transparently alongside the collection table.

OpenSearch replication requires:

- An external OpenSearch cluster, running and reachable from HCD.
- HCD started with `--search` (or explicit JVM flags for `OpenSearchMutatorInterceptor`, `OpenSearchQueryHandler`, and `OpenSearchIndex`).
- An operator-created `CREATE CUSTOM INDEX … USING 'OpenSearchIndex'`.

Making `$match` depend on OpenSearch would break the zero-configuration guarantee. The two features have categorically different deployment footprints.

---

## 3. The hybrid / rerank pipeline is SAI-only

The BM25 design is a foundational building block for two higher-level features:

- **`$hybrid`**: fuses ANN vector results with BM25 lexical results in a single query.
- **`findAndReRank`** (planned): runs multiple find queries, joins the candidate sets, and calls a re-ranker.

Both of these pipelines consume a candidate set produced by the Data API's own internal ANN or SAI paths. The current OpenSearch integration plan **explicitly blocks rerank** for `$search` because the `expr(...)` result set is owned and pre-ranked by OpenSearch — the Data API cannot intercept it at the right point in the pipeline.

If `$match` were backed by OpenSearch, the entire `$hybrid` and rerank system would become unusable without OpenSearch infrastructure.

---

## 4. Sort semantics diverge

`$lexical` in the sort clause maps 1:1 to the new CQL `ORDER BY <col> BM25 OF '...'` feature:

```json
{ "sort": { "$lexical": "best cheese" } }
```

This is a **server-side ranked sort** — Cassandra itself orders the rows by BM25 score before returning them. It can be combined with other CQL sort terms and works inside the standard Data API sort pipeline.

OpenSearch returns hits pre-ranked by relevance — there is no equivalent CQL-level BM25 sort for the `expr(...)` path. The Data API receives rows in whatever order HCD returns them after matching PKs against Cassandra, which is not guaranteed to preserve OpenSearch's relevance ordering end-to-end. A client-side `sort` on an `$search` result discards OpenSearch's relevance ordering entirely.

These are two different sort contracts and cannot share a syntax.

---

## 5. Query shape diverges — they are different products

| Capability | `$match` (SAI) | `$search` (OpenSearch DSL) |
|---|---|---|
| Single-field analyzed text match | ✅ | ✅ |
| BM25 ranking in `sort` clause | ✅ via CQL `ORDER BY BM25 OF` | ❌ — relevance is implicit, not a sort key |
| Multi-field search (`multi_match`) | ❌ — `$lexical` is a single field | ✅ |
| Compound boolean queries (`bool` / `must` / `should`) | ❌ | ✅ |
| Fuzzy, phrase, wildcard with OpenSearch semantics | ❌ | ✅ |
| Aggregations | ❌ | ✅ (HCD already supports via CQL `expr`) |
| Works with `$hybrid` and rerank | ✅ | ❌ |
| Zero extra infrastructure | ✅ | ❌ — requires external OpenSearch cluster |
| Immediate consistency | ✅ | ❌ — ~1 second async lag |

These are not two implementations of the same capability — they are two different search products exposed through the same API surface. Merging them behind a single `$lexical` block with an "engine" switch would hide fundamentally different consistency, infrastructure, and query semantics from the user.

---

## 6. The right model: complementary, not competing

The correct framing is that `$match` and `$search` target **different user profiles**:

| Profile | Needs | Right operator |
|---|---|---|
| Developer building a standard app | Simple keyword filter, immediate consistency, works with hybrid + rerank, zero infra | `$lexical` / `$match` |
| Operator running a search-heavy workload | Advanced full-text: bool, fuzzy, multi-field; willing to run OpenSearch; eventual consistency acceptable | `$search` |

A single collection can even have both enabled simultaneously — SAI-indexed fields for `$match` + `$hybrid`, OpenSearch-indexed fields for `$search` on richer queries. The two operators are independent and serve different query shapes in the same collection.

---

## Conclusion

Attempting to route `$match` or `$lexical` through OpenSearch would:

1. Break the synchronous consistency contract users expect from all Data API operators.
2. Make `$lexical` (currently zero-infra, on-by-default) depend on an external cluster.
3. Destroy the `$hybrid` and rerank pipeline, which is one of the primary motivations for the BM25 design.
4. Conflate two query products with different sort contracts under one syntax.

**Keep `$match` / `$lexical` on SAI. Keep `$search` on OpenSearch. Expose both as first-class, independent operators.**
