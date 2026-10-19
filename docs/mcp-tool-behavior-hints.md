# MCP tool behavior hints

The tool catalog publishes all four MCP behavior hints for every registered tool. They describe the full range of accepted arguments, including collection and table behavior. They do not change execution, authorize requests, or deduplicate retries.

| Tools | Read only | Destructive | Idempotent | Open world |
| --- | --- | --- | --- | --- |
| findKeyspaces, findCollections, listTables, listTypes, listIndexes | true | false | true | false |
| findEmbeddingProviders, findRerankingProviders | true | false | true | false |
| countDocuments, estimatedDocumentCount | true | false | true | false |
| find, findOne, findAndRerank | true | false | false | true |
| createKeyspace, createCollection, createTable, createType | false | false | true | false |
| createIndex, createTextIndex, createVectorIndex | false | false | true | false |
| dropKeyspace, deleteCollection, dropTable, dropType, dropIndex | false | true | true | false |
| alterTable, alterType | false | true | false | false |
| deleteMany | false | true | false | false |
| deleteOne, findOneAndDelete | false | true | false | true |
| insertOne, insertMany | false | true | false | true |
| updateOne, updateMany, findOneAndUpdate, findOneAndReplace | false | true | false | true |

## Behavior behind the hints

Provider listing reads configured provider descriptions. It does not invoke embedding or reranking models. Schema validation and database or credential-service access remain within the managed service boundary. Open world identifies tools that can send input to an embedding or reranking provider.

Find and findOne can vectorize a sort clause, and findAndRerank can invoke both embedding and reranking. These tools do not change stored data, but repeated calls can repeat provider work and charges. Their idempotency hint stays false. Sortable single-document deletes can also vectorize a query.

Insert tools cover tables as well as collections. Table inserts can overwrite existing row values, so both tools carry the destructive hint. Missing collection IDs are generated per invocation. Inserts and updates can also vectorize document contents. None of these tools advertises safe replay.

Filtered deleteMany calls stop at the configured deletion limit. A repeated call can remove another batch. Single-document deletes can select a different matching document on a second call. Updates can increment values, replace data, or generate an upsert. A new RPC ID does not prevent any of those effects.

Named schema creates and drops have no additional schema effect when repeated with the same arguments and no intervening changes. Their idempotency hint does not promise the same response. Depending on command options, a second call can report that an object already exists or is missing. Alterations keep a conservative false idempotency hint across their supported operation forms and may remove columns or rename fields.

## Verification

McpToolAnnotationsTest reads the catalog over HTTP and checks all 35 tool definitions. The test covers every hint and fails if a new tool has no reviewed metadata. It does not execute mutations or contact a model provider.

The hint fields follow the [Quarkiverse annotation API](https://github.com/quarkiverse/quarkus-mcp-server/blob/2.0.3/core/runtime/src/main/java/io/quarkiverse/mcp/server/Tool.java) and the [MCP tool contract](https://modelcontextprotocol.io/specification/2026-07-28/server/tools).
