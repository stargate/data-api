# 4 — Test HCD and OpenSearch

Smoke test of the HCD → OpenSearch replication, using **CQL** and direct **OpenSearch** calls only (the Data API is not involved yet, see [5-Test Data API with OpenSearch (curl).md](5-Test%20Data%20API%20with%20OpenSearch%20%28curl%29.md)).

## Prerequisites

The stack is started and healthy, see [3-Start everything.md](3-Start%20everything.md). Run each section in order.

---

## 1 — Open a CQL shell

```bash
./zcqlsh.sh
```

All CQL below is run inside this shell.

---

## 2 — Create keyspace and table

```cql
CREATE KEYSPACE IF NOT EXISTS demo
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE demo;

CREATE TABLE IF NOT EXISTS products (
    id        UUID PRIMARY KEY,
    name      TEXT,
    category  TEXT,
    price     DOUBLE,
    in_stock  BOOLEAN
);
```

---

## 3 — Create the OpenSearch custom index

This tells HCD to replicate every write on `demo.products` to an OpenSearch
index called `demo-products`.

```cql
CREATE CUSTOM INDEX IF NOT EXISTS products_search
ON demo.products ()
USING 'OpenSearchIndex'
WITH OPTIONS = {
    'indexName':              'demo-products',
    'createIndexIfNotExists': 'true',
    'columnsToIndex':         'name,category,price,in_stock'
};
```

---

## 4 — Insert test rows

```cql
INSERT INTO demo.products (id, name, category, price, in_stock)
VALUES (uuid(), 'HCD Turbo Widget',   'widgets',   29.99, true);

INSERT INTO demo.products (id, name, category, price, in_stock)
VALUES (uuid(), 'OpenSearch Gadget',  'gadgets',   49.99, true);

INSERT INTO demo.products (id, name, category, price, in_stock)
VALUES (uuid(), 'Cassandra Doohickey','widgets',   9.99,  false);

INSERT INTO demo.products (id, name, category, price, in_stock)
VALUES (uuid(), 'Replication Thingy', 'gadgets',   99.99, true);

INSERT INTO demo.products (id, name, category, price, in_stock)
VALUES (uuid(), 'Budget Sprocket',    'sprockets', 4.99,  true);
```

Verify rows landed in Cassandra:

```cql
SELECT id, name, category, price, in_stock FROM demo.products;
```

Expected: 5 rows.

---

## 5 — Verify data in OpenSearch (curl)

Wait ~2 seconds for the replication flush, then run these from a terminal (not
inside cqlsh).

### 5.1 — Check the index exists

```bash
curl -s http://localhost:9200/demo-products | python3 -m json.tool
```

Expected: JSON with `"index": "demo-products"` and `"status": 200`.

### 5.2 — Count documents

```bash
curl -s http://localhost:9200/demo-products/_count | python3 -m json.tool
```

Expected:

```json
{
    "count": 5,
    "_shards": { ... }
}
```

### 5.3 — Fetch all documents

```bash
curl -s 'http://localhost:9200/demo-products/_search?pretty&size=10'
```

Expected: `"total": {"value": 5, ...}` and all 5 products in `hits.hits`.

### 5.4 — Full-text search on `name`

```bash
curl -s -X POST 'http://localhost:9200/demo-products/_search?pretty' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": {
      "match": { "name": "widget" }
    }
  }'
```

Expected: 2 hits — `HCD Turbo Widget` and `Cassandra Doohickey` (both contain
"widget" in the `name` field).

### 5.5 — Filter by category

```bash
curl -s -X POST 'http://localhost:9200/demo-products/_search?pretty' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": {
      "term": { "category.keyword": "gadgets" }
    }
  }'
```

Expected: 2 hits — `OpenSearch Gadget` and `Replication Thingy`.

### 5.6 — Range query on price

```bash
curl -s -X POST 'http://localhost:9200/demo-products/_search?pretty' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": {
      "range": { "price": { "gte": 25 } }
    }
  }'
```

Expected: 3 hits — `HCD Turbo Widget` (29.99), `OpenSearch Gadget` (49.99),
`Replication Thingy` (99.99).

---

## 6 — Query OpenSearch from CQL using `expr()`

Back inside `cqlsh` (or re-run `./zcqlsh.sh`):

### 6.1 — Match all (return every row via OpenSearch)

```cql
SELECT id, name, category, price
FROM demo.products
WHERE expr(products_search, '{"match_all": {}}');
```

Expected: 5 rows (same as a full table scan).

### 6.2 — Full-text search on `name`

```cql
SELECT id, name, price
FROM demo.products
WHERE expr(products_search, '{"match": {"name": "widget"}}');
```

Expected: 2 rows — the two products containing "widget".

### 6.3 — Term filter on `category`

```cql
SELECT id, name, category
FROM demo.products
WHERE expr(products_search, '{"term": {"category.keyword": "gadgets"}}');
```

Expected: 2 rows — `OpenSearch Gadget` and `Replication Thingy`.

### 6.4 — Boolean query (in-stock gadgets under $60)

```cql
SELECT id, name, price, in_stock
FROM demo.products
WHERE expr(products_search, '{
  "bool": {
    "must": [
      {"term":  {"category.keyword": "gadgets"}},
      {"term":  {"in_stock": true}},
      {"range": {"price": {"lte": 60}}}
    ]
  }
}');
```

Expected: 1 row — `OpenSearch Gadget` (49.99, in_stock=true).

### 6.5 — Aggregation: average price per category

```cql
SELECT * FROM demo.products
WHERE expr(products_search, '{
  "aggs": {
    "by_category": {
      "terms": {"field": "category.keyword"},
      "aggs":  {"avg_price": {"avg": {"field": "price"}}}
    }
  }
}');
```

Expected: 1 row, 1 column (`by_category`) containing a JSON blob with three
buckets — `widgets`, `gadgets`, `sprockets` — each with an `avg_price` value.

---

## 7 — Cleanup (optional)

```cql
DROP INDEX IF EXISTS demo.products_search;
DROP TABLE IF EXISTS demo.products;
DROP KEYSPACE IF EXISTS demo;
```

Drop the OpenSearch index from a terminal:

```bash
curl -s -X DELETE http://localhost:9200/demo-products | python3 -m json.tool
```

Expected: `{"acknowledged": true}`.
