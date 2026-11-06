# 6 — Test Data API with OpenSearch (Java client)

Runs the same scenarios as [5-Test Data API with OpenSearch (curl).md](5-Test%20Data%20API%20with%20OpenSearch%20%28curl%29.md), but driven by the **astra-db-java** client, through three integration test classes that live in the `astra-db-java` repository.

> **Draft.** This file is an initial skeleton, to be refined.

---

## Test classes

| Class | Scenarios | Mirrors |
|---|---|---|
| [`HCD_Collections_02_OpenSearchIT`](https://github.com/datastax/astra-db-java/blob/clun/hcd-to-opensearch-integration/astra-db-java/src/test/java/com/datastax/astra/test/integration/local/HCD_Collections_02_OpenSearchIT.java) | A: collection created with OpenSearch. B: OpenSearch added/dropped with `alterCollection` | curl scenarios A and B |
| [`HCD_Collections_03_OpenSearchEdgeCasesIT`](https://github.com/datastax/astra-db-java/blob/clun/hcd-to-opensearch-integration/astra-db-java/src/test/java/com/datastax/astra/test/integration/local/HCD_Collections_03_OpenSearchEdgeCasesIT.java) | Collection edge cases: multi-level nested objects, OpenSearch `nested` arrays, EJSON types (`$date`, `$uuid`, binary), native vector search alongside OpenSearch | n/a |
| [`HCD_Tables_02_OpenSearchIT`](https://github.com/datastax/astra-db-java/blob/clun/hcd-to-opensearch-integration/astra-db-java/src/test/java/com/datastax/astra/test/integration/local/HCD_Tables_02_OpenSearchIT.java) | Tables: `createOpenSearchIndex`, `$openSearch` on a table, `countDocuments` | curl scenario C |

All three extend `AbstractDataAPITest` and are annotated `@EnabledIfLocalAvailable`: they are skipped unless the Data API answers on the configured local endpoint.

They are in the `astra-db-java` repository, branch `clun/hcd-to-opensearch-integration`, package `com.datastax.astra.test.integration.local` (module `astra-db-java`, `src/test/java`).

---

## Prerequisites

- The stack is running and healthy, see [3-Start everything.md](3-Start%20everything.md): Data API on `http://localhost:8181`, OpenSearch on `http://localhost:9200`.
- **Java 17** or later (the client is compiled with `maven.plugin.compiler.source=17`) and **Maven**.
- **Git**.

---

## 1 — Build astra-db-java from the source

The Data API Java client must be built from the source, because the OpenSearch support (`OpenSearchOptions`, `OpenSearchQuery`, `Filters.openSearch(...)`) lives on a feature branch. The test classes above are on the same branch.

```bash
git clone git@github.com:datastax/astra-db-java.git
cd astra-db-java
git checkout clun/hcd-to-opensearch-integration
```

Build and install the client in the local Maven repository, without running the tests:

```bash
mvn clean install -DskipTests -pl astra-db-java -am
```

Expected: `BUILD SUCCESS`.

---

## 2 — Check the local test configuration

The tests read their configuration (`TestConfig`) from system properties, `test-config*.properties` files or environment variables. The defaults match this stack:

| Setting | Property | Default |
|---|---|---|
| Environment | `-Dtest.environment` or env `ASTRA_DB_JAVA_TEST_ENV` | `local` |
| Data API endpoint | `-Dlocal.endpoint` | `http://localhost:8181` |
| Username | `-Dlocal.username` | `cassandra` |
| Password | `-Dlocal.password` | `cassandra` |
| OpenSearch URL | `-Dopensearch.url` | `http://localhost:9200` |

No change is needed with the stack from [3-Start everything.md](3-Start%20everything.md).

---

## 3 — Run the tests

From the `astra-db-java` clone:

```bash
mvn test -pl astra-db-java \
  -Dtest='HCD_Collections_02_OpenSearchIT,HCD_Collections_03_OpenSearchEdgeCasesIT,HCD_Tables_02_OpenSearchIT'
```

To run a single class, pass only its name to `-Dtest`. To target another endpoint or OpenSearch URL:

```bash
mvn test -pl astra-db-java -Dtest=HCD_Collections_02_OpenSearchIT \
  -Dlocal.endpoint=http://localhost:8181 \
  -Dopensearch.url=http://localhost:9200
```

Expected: all tests pass. A class reported as skipped means the Data API did not answer on the local endpoint.

---

## 4 — Cleanup

The tests drop their own collections, tables and OpenSearch indexes at the end (`clun_opensearch` keyspace). If a run was interrupted, the next run cleans the leftovers at startup. Nothing else to do.

---

## To refine

- Replace the feature branch with a released `astra-db-java` version once the OpenSearch support is published.
- Confirm the exact `mvn` command (`test` vs `verify`, surefire/failsafe includes) and expected test counts per class.
- Add the expected output of a successful run.
