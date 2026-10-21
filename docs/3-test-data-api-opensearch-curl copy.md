# Data API → HCD → OpenSearch (cURL-only) Test Plan

Smoke test that verifies the full Data API and OpenSearch integration exclusively using **cURL commands** (no CQL statements).

---

## Table of Contents

| # | Scenario | Description |
|---|---|---|
| **Prerequisites** | — | HCD & OpenSearch stack + Data API running |
| **Connection Details** | — | Base URLs, token, default names |
| **Scenario A** | [Collections — Create with OpenSearch from the start](#scenario-a--collections--create-collection-with-opensearch-enabled) | Create keyspace, create `products` collection with `openSearch` enabled, insert 50 docs, verify replication, run `$openSearch` queries, cleanup |
| **A.1** | [Create Keyspace](#a1--create-keyspace-via-data-api) | `createKeyspace` |
| **A.2** | [Create Collection with OpenSearch](#a2--create-collection-with-opensearch-enabled) | `createCollection` with `openSearch` options block |
| **A.3** | [Verify OpenSearch Index](#a3--verify-opensearch-index-creation-and-inspection) | HEAD / GET / `_settings` / `_mapping` / `_cat/indices` |
| **A.4** | [Insert Documents](#a4--insert-documents-insertmany) | `insertMany` (3 batches of 50 docs) |
| **A.5** | [Read Documents](#a5--read-documents-via-data-api-find) | `find` (plain, no `$openSearch`) |
| **A.6** | [Verify Replication](#a6--verify-replication-in-opensearch) | OpenSearch `_count` + `_search` |
| **A.7** | [Search via `$openSearch`](#a7--search-via-data-api-search-operator) | `match_all`, full-text, paging, bool, range, multi-field |
| **A.8** | [Cleanup](#a8--cleanup-via-curl) | `deleteCollection`, `dropNamespace`, delete OS index |
| **Scenario B** | [Collections — Add OpenSearch after the fact (`alterCollection`)](#scenario-b--collections--add-opensearch-after-the-fact-altercollection) | Create a plain collection, insert documents, then enable OpenSearch via `alterCollection addOpenSearch`, verify indexing and `$openSearch`, drop OpenSearch, verify it is disabled |
| **B.1** | [Create Plain Collection](#b1--create-a-plain-collection-no-opensearch) | `createCollection` without `openSearch` block |
| **B.2** | [Insert Documents into Plain Collection](#b2--insert-documents-into-the-plain-collection) | `insertOne` + `insertMany` |
| **B.3** | [Verify `$openSearch` Is Rejected](#b3--verify-search-is-rejected-before-opensearch-is-enabled) | Confirm `OPEN_SEARCH_NOT_ENABLED` error |
| **B.4** | [Add OpenSearch via `alterCollection`](#b4--add-opensearch-via-altercollection) | `alterCollection addOpenSearch` |
| **B.5** | [Verify Schema Updated](#b5--verify-schema-updated-findcollections-explain) | `findCollections explain: true` shows `openSearch` block |
| **B.6** | [Verify New Documents Replicated](#b6--verify-new-documents-are-replicated) | Insert new doc, verify in OpenSearch, run `$openSearch` |
| **B.7** | [Drop OpenSearch via `alterCollection`](#b7--drop-opensearch-via-altercollection) | `alterCollection dropOpenSearch` |
| **B.8** | [Verify `$openSearch` Rejected After Drop](#b8--verify-search-is-rejected-after-drop) | Confirm error again |
| **B.9** | [Cleanup Scenario B](#b9--cleanup-scenario-b) | Drop collection |
| **Scenario C** | [Tables — Create OpenSearch Index on a Table](#scenario-c--tables--opensearch-index-on-a-table) | Create a typed table, create an OpenSearch index on it, insert rows, run `$openSearch`, `countDocuments`, drop the index |
| **C.1** | [Create Table](#c1--create-table) | `createTable` with typed columns |
| **C.2** | [Create OpenSearch Index on Table](#c2--create-opensearch-index-on-the-table) | `createOpenSearchIndex` |
| **C.3** | [Verify OS Index Created](#c3--verify-opensearch-index-created-in-opensearch) | `_settings`, `_mapping`, `_cat/indices` |
| **C.4** | [Insert Rows into Table](#c4--insert-rows-into-the-table) | `insertOne` / `insertMany` via table API |
| **C.5** | [Verify Replication](#c5--verify-replication-in-opensearch-table) | OpenSearch `_count` + `_search` |
| **C.6** | [Search via `$openSearch` on Table](#c6--search-via-search-on-the-table) | `find` with `$openSearch` on a table |
| **C.7** | [Count with `$openSearch`](#c7--countdocuments-with-search-on-table) | `countDocuments` with `$openSearch` filter |
| **C.8** | [Drop OpenSearch Index on Table](#c8--drop-opensearch-index-on-table) | `dropIndex` with `indexType: openSearch` |
| **C.9** | [Cleanup Scenario C](#c9--cleanup-scenario-c) | Drop table |

---

## Prerequisites

### 1 — HCD & OpenSearch stack running

Ensure HCD and OpenSearch containers are up and healthy:

```bash
podman ps --format 'table {{.Names}}\t{{.Status}}'
```

Expected — all rows `healthy` or `Up`:

```
NAMES       STATUS
opensearch  Up ... (healthy)
hcd         Up ... (healthy)
prom        Up ...
grafana     Up ...
```

If not the process is to 

- Start Podman
```asciidoc
podman machine start
```

- Run `start.sh`

```
./scripts/start.sh
```

- Start DATA API
```
./scripts/start_data_api_local.sh
```

### 2 — Data API running

Ensure Data API is running and listening on `http://localhost:8181`:

```bash
curl -s -X POST http://localhost:8181/v1 \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{"findKeyspaces": {}}' | python3 -m json.tool
```

Expected: JSON response containing keyspaces list with no errors.

---

## Connection Details

| Item | Value |
|---|---|
| Data API Base URL | `http://localhost:8181` |
| Token Header Key | `Token` |
| Token Header Value | `Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh` |
| Keyspace / Namespace | `clun_opensearch` |
| Collection Name (Scenario A) | `products` |
| Collection Name (Scenario B) | `orders` |
| Table Name (Scenario C) | `articles` |
| OpenSearch Base URL | `http://localhost:9200` |
| OS Index Name (Scenario A) | `catalog-products` |
| OS Index Name (Scenario B) | `catalog-orders` |
| OS Index Name (Scenario C) | `hcd_clun_opensearch_articles` (default) |

---

## Scenario A — Collections — Create Collection with OpenSearch Enabled

This is the baseline scenario: the collection is created with OpenSearch replication enabled from day one. Every document written after creation is automatically mirrored to OpenSearch.

### A.1 — Create Keyspace via Data API

Create the keyspace `clun_opensearch`:

```bash
curl -s -X POST http://localhost:8181/v1 \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "createKeyspace": {
      "name": "clun_opensearch"
    }
  }' | python3 -m json.tool
```

Expected:

```json
{
  "status": {
    "ok": 1
  }
}
```

Verify keyspace exists:

```bash
curl -s -X POST http://localhost:8181/v1 \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{"findKeyspaces": {}}' | python3 -m json.tool
```

Expected: `clun_opensearch` present in `status.keyspaces`.

---

### A.2 — Create Collection with OpenSearch Enabled

Create the `products` collection with OpenSearch index options:

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
  "createCollection": {
    "name": "products",
    "options": {
      "openSearch": {
        "enabled": true,
        "indexName": "catalog-products",
        "numShards": 3,
        "numReplicas": 1,
        "mappings": {
          "status": {
            "type": "keyword"
          },
          "customerName": {
            "type": "text"
          },
          "total": {
            "type": "double"
          },
          "shippingAddress": {
            "properties": {
              "city": {
                "type": "keyword"
              },
              "country": {
                "type": "keyword"
              },
              "postalCode": {
                "type": "keyword"
              }
            }
          }
        }
      }
    }
  }
}' | python3 -m json.tool
```

Expected:

```json
{
  "status": {
    "ok": 1
  }
}
```

Verify collection schema with `explain: true`:

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "findCollections": {
      "options": {
        "explain": true
      }
    }
  }' | python3 -m json.tool
```

Expected: Response includes `products` collection with the resolved `openSearch` configuration block (all fields present: `enabled`, `indexName`, `numShards`, `numReplicas`, `mappings`).

---

### A.3 — Verify OpenSearch Index Creation and Inspection

Immediately after collection creation, check the newly created OpenSearch index `catalog-products`.

#### A.3.1 — Check Index Existence (HEAD / GET)

```bash
curl -s -I http://localhost:9200/catalog-products
```

Expected HTTP status: `200 OK`.

Or fetch full metadata:

```bash
curl -s http://localhost:9200/catalog-products | python3 -m json.tool
```

#### A.3.2 — Check Index Settings (Shards & Replicas)

Verify that `number_of_shards` is `3` and `number_of_replicas` is `1`:

```bash
curl -s http://localhost:9200/catalog-products/_settings | python3 -m json.tool
```

Expected output snippet:

```json
{
  "catalog-products": {
    "settings": {
      "index": {
        "number_of_shards": "3",
        "number_of_replicas": "1",
        
        "replication": {
          "type": "DOCUMENT"
        },
        "provided_name": "catalog-products",
        "creation_date": "1790618524328",
        "uuid": "tJbDn_cLTTO_c7nZNtIz0w",
        "version": {
          "created": "137287827"
        }
      }
    }
  }
}
```

#### A.3.3 — Check Index Mappings

Inspect the generated mappings in OpenSearch:

```bash
curl -s http://localhost:9200/catalog-products/_mapping | python3 -m json.tool
```

Expected output snippet:

```json
{
  "catalog-products": {
    "mappings": {
      "properties": {
        "_id": {
          "type": "keyword"
        },
        "customerName": {
          "type": "text"
        },
        "shippingAddress": {
          "properties": {
            "city": {
              "type": "keyword"
            },
            "country": {
              "type": "keyword"
            },
            "postalCode": {
              "type": "keyword"
            }
          }
        },
        "status": {
          "type": "keyword"
        },
        "total": {
          "type": "double"
        }
      }
    }
  }
}
```

#### A.3.4 — Check Index Health and Stats

Check health status of the index:

```bash
curl -s 'http://localhost:9200/_cat/indices/catalog-products?v'
```

Expected: Index `catalog-products` is listed with 3 primary shards and health `green` or `yellow` (depending on cluster node count).

---

### A.4 — Insert Documents (`insertMany`)

Insert 50 documents into `clun_opensearch/products` across three batches (Data API `insertMany` limit is 20 per call). The dataset covers four statuses and three countries so that paging and search tests have enough data to be meaningful.

**Batch 1 — products 1–20:**

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/products \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
  "insertMany": {
    "documents": [
      { "_id": "product-1",  "status": "shipped",   "customerName": "Ada Lovelace",       "total": 149.99, "shippingAddress": { "city": "London",    "country": "GB", "postalCode": "EC1A 1BB" } },
      { "_id": "product-2",  "status": "pending",   "customerName": "Grace Hopper",       "total":  89.50, "shippingAddress": { "city": "New York",  "country": "US", "postalCode": "10001"    } },
      { "_id": "product-3",  "status": "delivered", "customerName": "Alan Turing",        "total":   0.00, "shippingAddress": { "city": "Manchester","country": "GB", "postalCode": "M1 1AE"   } },
      { "_id": "product-4",  "status": "shipped",   "customerName": "Marie Curie",        "total": 210.00, "shippingAddress": { "city": "Paris",     "country": "FR", "postalCode": "75005"    } },
      { "_id": "product-5",  "status": "pending",   "customerName": "Nikola Tesla",       "total":  55.00, "shippingAddress": { "city": "New York",  "country": "US", "postalCode": "10002"    } },
      { "_id": "product-6",  "status": "cancelled", "customerName": "Charles Babbage",    "total":  33.00, "shippingAddress": { "city": "London",    "country": "GB", "postalCode": "WC2N 5DU" } },
      { "_id": "product-7",  "status": "shipped",   "customerName": "Lise Meitner",       "total": 175.50, "shippingAddress": { "city": "Paris",     "country": "FR", "postalCode": "75006"    } },
      { "_id": "product-8",  "status": "delivered", "customerName": "Max Planck",         "total":  99.99, "shippingAddress": { "city": "London",    "country": "GB", "postalCode": "SW1A 1AA" } },
      { "_id": "product-9",  "status": "pending",   "customerName": "Emmy Noether",       "total":  42.00, "shippingAddress": { "city": "New York",  "country": "US", "postalCode": "10003"    } },
      { "_id": "product-10", "status": "shipped",   "customerName": "Richard Feynman",    "total": 320.00, "shippingAddress": { "city": "Paris",     "country": "FR", "postalCode": "75007"    } },
      { "_id": "product-11", "status": "delivered", "customerName": "Rosalind Franklin",  "total":  67.25, "shippingAddress": { "city": "London",    "country": "GB", "postalCode": "E1 6AN"   } },
      { "_id": "product-12", "status": "cancelled", "customerName": "Werner Heisenberg",  "total":  15.00, "shippingAddress": { "city": "New York",  "country": "US", "postalCode": "10004"    } },
      { "_id": "product-13", "status": "shipped",   "customerName": "Niels Bohr",         "total": 499.00, "shippingAddress": { "city": "Paris",     "country": "FR", "postalCode": "75008"    } },
      { "_id": "product-14", "status": "pending",   "customerName": "Dorothy Hodgkin",    "total":  88.00, "shippingAddress": { "city": "London",    "country": "GB", "postalCode": "WC1E 6BT" } },
      { "_id": "product-15", "status": "delivered", "customerName": "Paul Dirac",         "total": 130.75, "shippingAddress": { "city": "New York",  "country": "US", "postalCode": "10005"    } },
      { "_id": "product-16", "status": "shipped",   "customerName": "Erwin Schrodinger",  "total": 250.00, "shippingAddress": { "city": "Paris",     "country": "FR", "postalCode": "75009"    } },
      { "_id": "product-17", "status": "cancelled", "customerName": "Enrico Fermi",       "total":  22.50, "shippingAddress": { "city": "London",    "country": "GB", "postalCode": "EC4M 5UT" } },
      { "_id": "product-18", "status": "shipped",   "customerName": "James Maxwell",      "total": 189.00, "shippingAddress": { "city": "New York",  "country": "US", "postalCode": "10006"    } },
      { "_id": "product-19", "status": "pending",   "customerName": "Cecilia Payne",      "total":  74.99, "shippingAddress": { "city": "Paris",     "country": "FR", "postalCode": "75010"    } },
      { "_id": "product-20", "status": "delivered", "customerName": "George Stokes",      "total":  60.00, "shippingAddress": { "city": "London",    "country": "GB", "postalCode": "N1 9GU"   } }
    ]
  }
}' | python3 -m json.tool
```

Expected: `"insertedIds"` contains `product-1` through `product-20`.

**Batch 2 — products 21–40:**

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/products \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
  "insertMany": {
    "documents": [
      { "_id": "product-21", "status": "shipped",   "customerName": "William Thomson",    "total": 310.00, "shippingAddress": { "city": "New York",  "country": "US", "postalCode": "10007"    } },
      { "_id": "product-22", "status": "pending",   "customerName": "Sofia Kovalevskaya", "total":  95.00, "shippingAddress": { "city": "Paris",     "country": "FR", "postalCode": "75011"    } },
      { "_id": "product-23", "status": "delivered", "customerName": "Ernest Rutherford",  "total": 145.00, "shippingAddress": { "city": "London",    "country": "GB", "postalCode": "WC2A 1HP" } },
      { "_id": "product-24", "status": "cancelled", "customerName": "Irene Curie",        "total":  18.00, "shippingAddress": { "city": "New York",  "country": "US", "postalCode": "10008"    } },
      { "_id": "product-25", "status": "shipped",   "customerName": "J Robert Oppenheimer","total": 420.50,"shippingAddress": { "city": "Paris",     "country": "FR", "postalCode": "75012"    } },
      { "_id": "product-26", "status": "pending",   "customerName": "Vera Rubin",         "total":  51.00, "shippingAddress": { "city": "London",    "country": "GB", "postalCode": "SE1 7PB"  } },
      { "_id": "product-27", "status": "delivered", "customerName": "Stephen Hawking",    "total": 200.00, "shippingAddress": { "city": "New York",  "country": "US", "postalCode": "10009"    } },
      { "_id": "product-28", "status": "shipped",   "customerName": "Carl Sagan",         "total": 375.00, "shippingAddress": { "city": "Paris",     "country": "FR", "postalCode": "75013"    } },
      { "_id": "product-29", "status": "cancelled", "customerName": "Murray Gell-Mann",   "total":  29.99, "shippingAddress": { "city": "London",    "country": "GB", "postalCode": "W1B 5TF"  } },
      { "_id": "product-30", "status": "shipped",   "customerName": "Richard Dawkins",    "total": 112.00, "shippingAddress": { "city": "New York",  "country": "US", "postalCode": "10010"    } },
      { "_id": "product-31", "status": "delivered", "customerName": "Claude Shannon",     "total":  78.50, "shippingAddress": { "city": "Paris",     "country": "FR", "postalCode": "75014"    } },
      { "_id": "product-32", "status": "pending",   "customerName": "John von Neumann",   "total": 560.00, "shippingAddress": { "city": "London",    "country": "GB", "postalCode": "EC1V 9HX" } },
      { "_id": "product-33", "status": "shipped",   "customerName": "Alonzo Church",      "total": 230.00, "shippingAddress": { "city": "New York",  "country": "US", "postalCode": "10011"    } },
      { "_id": "product-34", "status": "cancelled", "customerName": "Kurt Godel",         "total":  40.00, "shippingAddress": { "city": "Paris",     "country": "FR", "postalCode": "75015"    } },
      { "_id": "product-35", "status": "delivered", "customerName": "John Nash",          "total":  92.00, "shippingAddress": { "city": "London",    "country": "GB", "postalCode": "SE11 5QY" } },
      { "_id": "product-36", "status": "shipped",   "customerName": "Bertrand Russell",   "total": 185.00, "shippingAddress": { "city": "New York",  "country": "US", "postalCode": "10012"    } },
      { "_id": "product-37", "status": "pending",   "customerName": "David Hilbert",      "total":  63.00, "shippingAddress": { "city": "Paris",     "country": "FR", "postalCode": "75016"    } },
      { "_id": "product-38", "status": "delivered", "customerName": "Leonhard Euler",     "total": 290.00, "shippingAddress": { "city": "London",    "country": "GB", "postalCode": "WC1N 3AX" } },
      { "_id": "product-39", "status": "shipped",   "customerName": "Carl Gauss",         "total": 455.00, "shippingAddress": { "city": "New York",  "country": "US", "postalCode": "10013"    } },
      { "_id": "product-40", "status": "cancelled", "customerName": "Pierre de Fermat",   "total":  11.00, "shippingAddress": { "city": "Paris",     "country": "FR", "postalCode": "75017"    } }
    ]
  }
}' | python3 -m json.tool
```

Expected: `"insertedIds"` contains `product-21` through `product-40`.

**Batch 3 — products 41–50:**

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/products \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
  "insertMany": {
    "documents": [
      { "_id": "product-41", "status": "shipped",   "customerName": "Blaise Pascal",      "total": 340.00, "shippingAddress": { "city": "London",    "country": "GB", "postalCode": "E14 5AB"  } },
      { "_id": "product-42", "status": "delivered", "customerName": "Isaac Newton",       "total": 615.00, "shippingAddress": { "city": "New York",  "country": "US", "postalCode": "10014"    } },
      { "_id": "product-43", "status": "pending",   "customerName": "Gottfried Leibniz",  "total":  47.50, "shippingAddress": { "city": "Paris",     "country": "FR", "postalCode": "75018"    } },
      { "_id": "product-44", "status": "cancelled", "customerName": "Rene Descartes",     "total":  36.00, "shippingAddress": { "city": "London",    "country": "GB", "postalCode": "EC2Y 5AS" } },
      { "_id": "product-45", "status": "shipped",   "customerName": "Euclid",             "total": 270.00, "shippingAddress": { "city": "New York",  "country": "US", "postalCode": "10015"    } },
      { "_id": "product-46", "status": "delivered", "customerName": "Archimedes",         "total": 390.00, "shippingAddress": { "city": "Paris",     "country": "FR", "postalCode": "75019"    } },
      { "_id": "product-47", "status": "pending",   "customerName": "Pythagoras",         "total":  58.00, "shippingAddress": { "city": "London",    "country": "GB", "postalCode": "W8 6EJ"   } },
      { "_id": "product-48", "status": "shipped",   "customerName": "Hypatia",            "total": 125.00, "shippingAddress": { "city": "New York",  "country": "US", "postalCode": "10016"    } },
      { "_id": "product-49", "status": "delivered", "customerName": "Al-Khwarizmi",       "total": 480.00, "shippingAddress": { "city": "Paris",     "country": "FR", "postalCode": "75020"    } },
      { "_id": "product-50", "status": "cancelled", "customerName": "Ada Lovelace II",    "total":  25.00, "shippingAddress": { "city": "London",    "country": "GB", "postalCode": "SW7 2AZ"  } }
    ]
  }
}' | python3 -m json.tool
```

Expected: `"insertedIds"` contains `product-41` through `product-50`.

---

### A.5 — Read Documents via Data API (`find`)

Verify documents are readable via standard Data API `find` (returns up to 20 per page by default):

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/products \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "find": {
      "filter": {}
    }
  }' | python3 -m json.tool
```

Expected: 20 documents returned in `data.documents` and a `nextPageState` token present (50 total documents exist).

---

### A.6 — Verify Replication in OpenSearch

Wait ~1–2 seconds for the HCD bulk flush, then run OpenSearch REST API calls via cURL.

#### A.6.1 — Verify Index Exists and Check Mappings

```bash
curl -s http://localhost:9200/catalog-products | python3 -m json.tool
```

Expected: Response includes index configuration, settings (`number_of_shards: 3`, `number_of_replicas: 1`), and mappings.

#### A.6.2 — Count Documents in OpenSearch

```bash
curl -s http://localhost:9200/catalog-products/_count | python3 -m json.tool
```

Expected:

```json
{
  "count": 50,
  "_shards": {
    "total": 3,
    "successful": 3,
    "skipped": 0,
    "failed": 0
  }
}
```

#### A.6.3 — Search All Documents in OpenSearch

```bash
curl -s 'http://localhost:9200/catalog-products/_search?pretty&size=50'
```

Expected: `hits.total.value` is `50`, with document hits showing replicated fields.

---

### A.7 — Search via Data API `$openSearch` Operator

Query OpenSearch through the Data API `$openSearch` filter operator:

#### A.7.1 — Match all documents via Data API

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/products \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "find": {
      "filter": {
        "$openSearch": {
          "match_all": {}
        }
      }
    }
  }' | python3 -m json.tool
```

Expected: Up to 20 documents returned (first page of 50) with a `nextPageState` token.

#### A.7.1b — Match all via CQL `expr()` (direct HCD / cqlsh verification)

As a low-level sanity check you can query the same OpenSearch index directly from CQL using the `expr()` function.

The Data API auto-generates the Cassandra index name using the pattern `hcd_<keyspace>_<collection>`:

```cql
SELECT key, doc_json
FROM clun_opensearch.products
WHERE expr(hcd_clun_opensearch_products, '{"match_all": {}}');
```

> **Contrast with test plan 2:** there the index was created manually with an explicit name (`persons_os_index`). Here the Data API owns index creation and follows the `hcd_<keyspace>_<collection>` naming convention — no hyphens, no quoting needed.

Expected: All rows returned with their `doc_json` payload.

---

#### A.7.2 — Full-text search via Data API

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/products \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "find": {
      "filter": {
        "$openSearch": {
          "match": {
            "customerName": "Lovelace"
          }
        }
      }
    }
  }' | python3 -m json.tool
```

Expected: 1 document returned (`product-1`).

---

#### A.7.3 — Paging via `pageState`

With 50 documents indexed, a `limit: 20` request returns the first page and a cursor token for the next. This is the **only supported paging mechanism** for `$openSearch`.

**Page 1:**

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/products \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "find": {
      "filter": {
        "$openSearch": {
          "match_all": {}
        }
      },
      "options": { "limit": 20 }
    }
  }' | python3 -m json.tool
```

Expected: 20 documents in `data.documents` and a `nextPageState` string in the response.

**Page 2** — copy the `nextPageState` value from the previous response and paste it below:

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/products \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "find": {
      "filter": {
        "$openSearch": {
          "match_all": {}
        }
      },
      "options": { "limit": 20, "pageState": "<nextPageState from page 1>" }
    }
  }' | python3 -m json.tool
```

Expected: next 20 documents, a new `nextPageState` token.

**Page 3** — repeat with the token from page 2:

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/products \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "find": {
      "filter": {
        "$openSearch": {
          "match_all": {}
        }
      },
      "options": { "limit": 20, "pageState": "<nextPageState from page 2>" }
    }
  }' | python3 -m json.tool
```

Expected: final 10 documents, **no** `nextPageState` (end of results).

> **How it works:** `options.limit` sets the Cassandra driver page size. HCD re-runs the OpenSearch query on each CQL page fetch and returns the next slice of matching PKs via Cassandra's native cursor. `from`/`size` and `search_after` inside the `$openSearch` DSL are **not supported**.

#### A.7.4 — Bool query — filter by status and country

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/products \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "find": {
      "filter": {
        "$openSearch": {
          "bool": {
            "must": [
              { "term": { "status": "shipped" } }
            ],
            "filter": [
              { "term": { "shippingAddress.country": "FR" } }
            ]
          }
        }
      }
    }
  }' | python3 -m json.tool
```

Expected: Documents with `status=shipped` AND `country=FR` (product-4, product-7, product-10, product-13, product-16, product-25, product-28, product-33, product-39).

#### A.7.5 — Range query — orders with `total` between 100 and 300

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/products \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "find": {
      "filter": {
        "$openSearch": {
          "range": {
            "total": {
              "gte": 100.0,
              "lte": 300.0
            }
          }
        }
      }
    }
  }' | python3 -m json.tool
```

Expected: All documents with `total` in `[100, 300]`.

#### A.7.6 — Multi-field match — search `customerName` across tokens

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/products \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "find": {
      "filter": {
        "$openSearch": {
          "multi_match": {
            "query": "Newton",
            "fields": ["customerName"]
          }
        }
      }
    }
  }' | python3 -m json.tool
```

Expected: 1 document returned — `product-42` (Isaac Newton).

#### A.7.7 — $openSearch combined with projection

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/products \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "find": {
      "filter": {
        "$openSearch": {
          "term": { "status": "cancelled" }
        }
      },
      "projection": {
        "_id": 1,
        "customerName": 1,
        "total": 1
      }
    }
  }' | python3 -m json.tool
```

Expected: Only `_id`, `customerName`, and `total` fields returned for cancelled orders (no `shippingAddress`).

---

### A.8 — Cleanup via cURL (Optional)

#### A.8.1 — Delete Collection via Data API

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "deleteCollection": {
      "name": "products"
    }
  }' | python3 -m json.tool
```

Expected: `{"status": {"ok": 1}}`

#### A.8.2 — Drop Keyspace / Namespace via Data API

```bash
curl -s -X POST http://localhost:8181/v1 \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "dropNamespace": {
      "name": "clun_opensearch"
    }
  }' | python3 -m json.tool
```

Expected: `{"status": {"ok": 1}}`

#### A.8.3 — Delete OpenSearch Index (if not already dropped)

```bash
curl -s -X DELETE http://localhost:9200/catalog-products | python3 -m json.tool
```

Expected: `{"acknowledged": true}`

---

## Scenario B — Collections — Add OpenSearch After the Fact (`alterCollection`)

This scenario tests a different lifecycle: the collection is created **without** OpenSearch, data is inserted, and OpenSearch is then enabled later via `alterCollection addOpenSearch`. This validates that:
1. `$openSearch` is correctly rejected before OpenSearch is added.
2. `alterCollection addOpenSearch` creates the OS index and updates the schema.
3. New writes after `addOpenSearch` are mirrored to OpenSearch (pre-existing documents are **not** automatically backfilled — that is out of scope for this release).
4. `alterCollection dropOpenSearch` removes the index and `$openSearch` is rejected again.

> **Keyspace reuse:** This scenario reuses the `clun_opensearch` keyspace. Re-run section A.1 first if the keyspace was cleaned up.

---

### B.1 — Create a Plain Collection (no OpenSearch)

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "createCollection": {
      "name": "orders"
    }
  }' | python3 -m json.tool
```

Expected:

```json
{
  "status": {
    "ok": 1
  }
}
```

Verify the collection exists but has **no** `openSearch` block:

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "findCollections": {
      "options": { "explain": true }
    }
  }' | python3 -m json.tool
```

Expected: `orders` collection is present and its `options` object contains **no** `openSearch` key.

---

### B.2 — Insert Documents into the Plain Collection

Insert a small dataset of 5 orders into the plain `orders` collection:

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/orders \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "insertMany": {
      "documents": [
        { "_id": "order-1", "status": "pending",   "customerName": "Alice Martin",  "total": 99.00,  "city": "Paris"   },
        { "_id": "order-2", "status": "shipped",   "customerName": "Bob Smith",     "total": 149.50, "city": "London"  },
        { "_id": "order-3", "status": "delivered", "customerName": "Carol Johnson", "total": 49.99,  "city": "New York"},
        { "_id": "order-4", "status": "cancelled", "customerName": "David Brown",   "total": 12.00,  "city": "Paris"   },
        { "_id": "order-5", "status": "pending",   "customerName": "Eve Wilson",    "total": 220.00, "city": "London"  }
      ]
    }
  }' | python3 -m json.tool
```

Expected: `"insertedIds"` contains `order-1` through `order-5`.

Verify via plain `find`:

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/orders \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{"find": {"filter": {}}}' | python3 -m json.tool
```

Expected: 5 documents returned.

---

### B.3 — Verify `$openSearch` Is Rejected Before OpenSearch Is Enabled

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/orders \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "find": {
      "filter": {
        "$openSearch": {
          "match": { "customerName": "Alice" }
        }
      }
    }
  }' | python3 -m json.tool
```

Expected error:

```json
{
  "errors": [
    {
      "message": "...",
      "errorCode": "OPEN_SEARCH_NOT_ENABLED"
    }
  ]
}
```

---

### B.4 — Add OpenSearch via `alterCollection`

Add an OpenSearch replication index to the existing `orders` collection. Fields: `status` (keyword), `customerName` (text), `total` (double), `city` (keyword).

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/orders \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "alterCollection": {
      "operation": {
        "addOpenSearch": {
          "enabled": true,
          "indexName": "catalog-orders",
          "numShards": 1,
          "numReplicas": 1,
          "mappings": {
            "status":       { "type": "keyword" },
            "customerName": { "type": "text"    },
            "total":        { "type": "double"  },
            "city":         { "type": "keyword" }
          }
        }
      }
    }
  }' | python3 -m json.tool
```

Expected:

```json
{
  "status": {
    "ok": 1
  }
}
```

Verify the OpenSearch index was created in OpenSearch:

```bash
curl -s -I http://localhost:9200/catalog-orders
```

Expected HTTP status: `200 OK`.

---

### B.5 — Verify Schema Updated (`findCollections explain`)

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "findCollections": {
      "options": { "explain": true }
    }
  }' | python3 -m json.tool
```

Expected: `orders` collection now contains a full `openSearch` block:

```json
{
  "name": "orders",
  "options": {
    "openSearch": {
      "enabled": true,
      "indexName": "catalog-orders",
      "numShards": 1,
      "numReplicas": 1,
      "mappings": {
        "status":       { "type": "keyword" },
        "customerName": { "type": "text"    },
        "total":        { "type": "double"  },
        "city":         { "type": "keyword" }
      }
    }
  }
}
```

---

### B.6 — Verify New Documents Are Replicated

Insert a new document **after** `addOpenSearch` completed, then verify it appears in OpenSearch and is searchable:

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/orders \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "insertOne": {
      "document": {
        "_id": "order-6",
        "status": "shipped",
        "customerName": "Frank Ocean",
        "total": 77.77,
        "city": "Paris"
      }
    }
  }' | python3 -m json.tool
```

Expected: `"insertedId": "order-6"`.

Wait ~2 seconds for HCD bulk flush, then verify in OpenSearch:

```bash
curl -s http://localhost:9200/catalog-orders/_count | python3 -m json.tool
```

Expected: `"count": 1` (only `order-6` — pre-existing documents written before `addOpenSearch` are **not** backfilled).

Now search for the new document:

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/orders \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "find": {
      "filter": {
        "$openSearch": {
          "match": { "customerName": "Ocean" }
        }
      }
    }
  }' | python3 -m json.tool
```

Expected: 1 document returned — `order-6`.

---

### B.7 — Drop OpenSearch via `alterCollection`

Remove the OpenSearch replication index from the `orders` collection:

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/orders \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "alterCollection": {
      "operation": {
        "dropOpenSearch": {}
      }
    }
  }' | python3 -m json.tool
```

Expected:

```json
{
  "status": {
    "ok": 1
  }
}
```

Verify the OS index was deleted from OpenSearch:

```bash
curl -s -I http://localhost:9200/catalog-orders
```

Expected HTTP status: `404 Not Found`.

Verify schema updated — `openSearch` block removed:

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "findCollections": {
      "options": { "explain": true }
    }
  }' | python3 -m json.tool
```

Expected: `orders` collection `options` object contains **no** `openSearch` key.

---

### B.8 — Verify `$openSearch` Is Rejected After Drop

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/orders \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "find": {
      "filter": {
        "$openSearch": {
          "match": { "customerName": "Ocean" }
        }
      }
    }
  }' | python3 -m json.tool
```

Expected error:

```json
{
  "errors": [
    {
      "message": "...",
      "errorCode": "OPEN_SEARCH_NOT_ENABLED_FOR_COLLECTION"
    }
  ]
}
```

---

### B.9 — Cleanup Scenario B

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "deleteCollection": {
      "name": "orders"
    }
  }' | python3 -m json.tool
```

Expected: `{"status": {"ok": 1}}`

---

## Scenario C — Tables — OpenSearch Index on a Table

This scenario covers the **Tables** path, which is fundamentally different from Collections:
- Tables have **typed columns** (no `doc_json` blob), so HCD can auto-derive the OpenSearch mapping from the Cassandra schema (`applyDefaultSchema: true` is the default).
- The OpenSearch index is created with a dedicated `createOpenSearchIndex` command (not via `createCollection`).
- The **SAI index name** (the Cassandra custom index identifier, used in `expr(...)`) is **user-supplied** via the `name` field — unlike collections where it is always auto-generated as `hcd_<keyspace>_<collectionName>`.
- Dropping the index uses `dropIndex` with `indexType: openSearch`.

> **Keyspace reuse:** This scenario reuses the `clun_opensearch` keyspace.

---

### C.1 — Create Table

Create a typed `articles` table with columns suitable for full-text search:

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "createTable": {
      "name": "articles",
      "definition": {
        "columns": {
          "article_id": { "type": "text" },
          "title":      { "type": "text" },
          "body":       { "type": "text" },
          "author":     { "type": "text" },
          "category":   { "type": "text" },
          "word_count": { "type": "int"  },
          "published":  { "type": "boolean" }
        },
        "primaryKey": {
          "partitionBy": ["article_id"]
        }
      }
    }
  }' | python3 -m json.tool
```

Expected:

```json
{
  "status": {
    "ok": 1
  }
}
```

---

### C.2 — Create OpenSearch Index on the Table

Create a `createOpenSearchIndex` with:
- **SAI index name:** `articles_search` (user-supplied; used in `expr(articles_search, ...)` queries)
- **Columns to replicate:** `title`, `body`, `author`, `category`
- **OS index name:** default (`hcd_clun_opensearch_articles`) — omitted here so HCD uses the auto-generated name
- **Mappings:** explicit types to override HCD default schema

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "createOpenSearchIndex": {
      "name": "articles_search",
      "definition": {
        "columns": ["title", "body", "author", "category", "word_count", "published"],
        "options": {
          "mappings": {
            "title":      { "type": "text",    "analyzer": "english" },
            "body":       { "type": "text",    "analyzer": "english" },
            "author":     { "type": "keyword" },
            "category":   { "type": "keyword" },
            "word_count": { "type": "integer" },
            "published":  { "type": "boolean" }
          }
        }
      },
      "options": {
        "ifNotExists": true
      }
    }
  }' | python3 -m json.tool
```

Expected:

```json
{
  "status": {
    "ok": 1
  }
}
```

Verify index appears in `listIndexes`:

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/articles \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{"listIndexes": {}}' | python3 -m json.tool
```

Expected: Response includes `articles_search` with `indexType: "openSearch"`.

---

### C.3 — Verify OpenSearch Index Created in OpenSearch

#### C.3.1 — Check Index Existence

```bash
curl -s -I http://localhost:9200/hcd_clun_opensearch_articles
```

Expected HTTP status: `200 OK`.

#### C.3.2 — Check Mappings

```bash
curl -s http://localhost:9200/hcd_clun_opensearch_articles/_mapping | python3 -m json.tool
```

Expected: Mappings include `title` (text), `body` (text), `author` (keyword), `category` (keyword), `word_count` (integer), `published` (boolean) — derived from either the explicit `mappings` block or Cassandra column types.

#### C.3.3 — Check Health

```bash
curl -s 'http://localhost:9200/_cat/indices/hcd_clun_opensearch_articles?v'
```

Expected: Index listed, health `green` or `yellow`.

---

### C.4 — Insert Rows into the Table

Insert 10 article rows using the Data API table insert:

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/articles \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "insertMany": {
      "documents": [
        { "article_id": "art-1",  "title": "Introduction to Cassandra",           "body": "Apache Cassandra is a distributed NoSQL database designed for high availability.",         "author": "Alice",   "category": "database",    "word_count": 850,  "published": true  },
        { "article_id": "art-2",  "title": "OpenSearch Full-Text Search Guide",   "body": "OpenSearch is an open source search and analytics engine built on Apache Lucene.",         "author": "Bob",     "category": "search",      "word_count": 1200, "published": true  },
        { "article_id": "art-3",  "title": "HCD and OpenSearch Integration",      "body": "HCD mirrors Cassandra writes to OpenSearch in real time using a write-path interceptor.",  "author": "Carol",   "category": "integration", "word_count": 980,  "published": true  },
        { "article_id": "art-4",  "title": "Data API Collections vs Tables",      "body": "Collections store schemaless JSON while tables use typed columns with a defined schema.",  "author": "David",   "category": "api",         "word_count": 720,  "published": true  },
        { "article_id": "art-5",  "title": "Distributed Systems Fundamentals",   "body": "Consistency, availability, and partition tolerance form the cornerstone of distributed databases.", "author": "Eve", "category": "database",    "word_count": 1500, "published": false },
        { "article_id": "art-6",  "title": "Lucene Query Syntax",                "body": "The Lucene query parser supports fields, wildcards, fuzzy searches, and boolean operators.", "author": "Frank",   "category": "search",      "word_count": 640,  "published": true  },
        { "article_id": "art-7",  "title": "CQL Primer for Beginners",           "body": "Cassandra Query Language is modelled after SQL but designed for the Cassandra data model.", "author": "Grace",   "category": "database",    "word_count": 900,  "published": true  },
        { "article_id": "art-8",  "title": "Real-Time Data Pipelines",           "body": "Modern data pipelines replicate changes using CDC and streaming platforms like Kafka.",     "author": "Henry",   "category": "integration", "word_count": 1100, "published": true  },
        { "article_id": "art-9",  "title": "Kubernetes for Data Engineers",      "body": "Running stateful databases on Kubernetes requires careful attention to storage and networking.", "author": "Iris", "category": "ops",         "word_count": 1350, "published": false },
        { "article_id": "art-10", "title": "Relevance Scoring in OpenSearch",    "body": "BM25 is the default relevance algorithm in OpenSearch, replacing TF-IDF from earlier Elasticsearch versions.", "author": "Jack", "category": "search", "word_count": 760, "published": true }
      ]
    }
  }' | python3 -m json.tool
```

Expected: `"insertedIds"` contains `art-1` through `art-10`.

---

### C.5 — Verify Replication in OpenSearch (Table)

Wait ~2 seconds, then:

#### C.5.1 — Count Documents in OpenSearch

```bash
curl -s http://localhost:9200/hcd_clun_opensearch_articles/_count | python3 -m json.tool
```

Expected: `"count": 10`.

#### C.5.2 — Search All in OpenSearch

```bash
curl -s 'http://localhost:9200/hcd_clun_opensearch_articles/_search?pretty&size=10'
```

Expected: `hits.total.value` is `10`, hits show `title`, `author`, `category` etc. in `_source`.

---

### C.6 — Search via `$openSearch` on the Table

#### C.6.1 — Full-text match on `title`

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/articles \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "find": {
      "filter": {
        "$openSearch": {
          "match": { "title": "Cassandra" }
        }
      }
    }
  }' | python3 -m json.tool
```

Expected: Articles mentioning "Cassandra" in the title — `art-1` (Introduction to Cassandra), `art-7` (CQL Primer).

#### C.6.2 — Full-text match on `body`

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/articles \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "find": {
      "filter": {
        "$openSearch": {
          "match": { "body": "OpenSearch replication" }
        }
      }
    }
  }' | python3 -m json.tool
```

Expected: `art-3` (HCD and OpenSearch Integration) at minimum.

#### C.6.3 — Filter by category (keyword exact match)

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/articles \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "find": {
      "filter": {
        "$openSearch": {
          "term": { "category": "search" }
        }
      }
    }
  }' | python3 -m json.tool
```

Expected: 3 documents — `art-2`, `art-6`, `art-10` (all `category=search`).

#### C.6.4 — Bool query — published articles about databases

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/articles \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "find": {
      "filter": {
        "$openSearch": {
          "bool": {
            "must": [
              { "term": { "category": "database" } }
            ],
            "filter": [
              { "term": { "published": true } }
            ]
          }
        }
      }
    }
  }' | python3 -m json.tool
```

Expected: `art-1` and `art-7` (category=database and published=true; `art-5` is excluded because `published=false`).

#### C.6.5 — Range query — articles with `word_count` over 1000

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/articles \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "find": {
      "filter": {
        "$openSearch": {
          "range": {
            "word_count": { "gt": 1000 }
          }
        }
      }
    }
  }' | python3 -m json.tool
```

Expected: `art-2` (1200), `art-5` (1500), `art-8` (1100), `art-9` (1350).

#### C.6.6 — Multi-match across `title` and `body`

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/articles \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "find": {
      "filter": {
        "$openSearch": {
          "multi_match": {
            "query": "distributed database",
            "fields": ["title", "body"]
          }
        }
      }
    }
  }' | python3 -m json.tool
```

Expected: Documents mentioning "distributed" or "database" in `title` or `body` — ranked by relevance.

---

### C.7 — `countDocuments` with `$openSearch` on Table

Count published articles:

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/articles \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "countDocuments": {
      "filter": {
        "$openSearch": {
          "term": { "published": true }
        }
      }
    }
  }' | python3 -m json.tool
```

Expected:

```json
{
  "status": {
    "count": 8
  }
}
```

(8 articles have `published: true` — `art-5` and `art-9` are `false`.)

---

### C.8 — Drop OpenSearch Index on Table

Drop the `articles_search` index using `dropIndex` with `indexType: openSearch`:

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/articles \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "dropIndex": {
      "name": "articles_search",
      "options": {
        "ifExists": true
      }
    }
  }' | python3 -m json.tool
```

Expected:

```json
{
  "status": {
    "ok": 1
  }
}
```

Verify the OS index is gone from OpenSearch:

```bash
curl -s -I http://localhost:9200/hcd_clun_opensearch_articles
```

Expected HTTP status: `404 Not Found`.

Verify `$openSearch` is now rejected on the table:

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch/articles \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "find": {
      "filter": {
        "$openSearch": {
          "match": { "title": "Cassandra" }
        }
      }
    }
  }' | python3 -m json.tool
```

Expected: Error response indicating no OpenSearch index is available.

---

### C.9 — Cleanup Scenario C

Drop the `articles` table:

```bash
curl -s -X POST http://localhost:8181/v1/clun_opensearch \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "dropTable": {
      "name": "articles",
      "options": { "ifExists": true }
    }
  }' | python3 -m json.tool
```

Expected: `{"status": {"ok": 1}}`

Drop keyspace if all scenarios are complete:

```bash
curl -s -X POST http://localhost:8181/v1 \
  -H 'Token: Cassandra:Y2Fzc2FuZHJh:Y2Fzc2FuZHJh' \
  -H 'Content-Type: application/json' \
  -d '{
    "dropNamespace": {
      "name": "clun_opensearch"
    }
  }' | python3 -m json.tool
```

Expected: `{"status": {"ok": 1}}`
