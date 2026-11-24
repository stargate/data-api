# Vectorize suite through MCP

The replay sends all four commands from [the pinned `vectorize-base` suite](https://github.com/stargate/data-api/blob/4e6a2dd4c11de5cb261db6f0327d68fbe2125877/src/test/resources/testbench/testsuites/vectorize-base.json) to the Data API MCP server over Streamable HTTP. It removes `sort.$vectorize` from the two query commands and drops each empty `sort` object. The inserted documents retain their `$vectorize` text.

## Run the integration replay

From the repository root, use Java 21 and the repository's Docker-backed integration test setup.

```sh
./mvnw -B -ntp verify -DskipUnitTests -Dit.test=VectorizeBaseMcpIntegrationTest
```

The test creates an isolated collection with the local test embedding provider and cleans up afterward. It checks the fixture's expected document count and update counts. The unsorted update can pick any of the three inserted movies.

## Run the same case with an agent

Connect the agent to the Data API's `/v1/mcp` endpoint over Streamable HTTP. Give it the keyspace and collection names for an empty, disposable vectorize-enabled collection. Configure the MCP client with the server's `Token` header and, for the local test embedding provider, `x-embedding-api-key: test_embedding_service_api_key`.

Use this prompt with the [suite fixture](../src/test/resources/testbench/testsuites/vectorize-base.json) and the target names filled in.

> Read the `vectorize-base.json` fixture. Discover the Data API tools from the connected MCP server. Run every setup command, then every test command, in file order against `<keyspace>.<collection>`. Translate each command into a call to the corresponding MCP tool. Remove `$vectorize` from each command's `sort` and omit `sort` if it becomes empty. Keep the insert documents, filters, updates, and options as written. Report the tool name and observed result for each call. Give the inserted and found IDs, the returned update document, and the matched and modified counts. Surface any tool error.

Grade the run from the MCP transcript. Expect `insertOne`, `insertMany`, `find`, and `findOneAndUpdate` in order. The inserts should contain all three original `$vectorize` strings. The query calls should omit `sort`, and `find` should return the three seeded IDs in any order. The update should return one of those movies with `status: "active"`, `matchedCount: 1`, and `modifiedCount: 1`.
