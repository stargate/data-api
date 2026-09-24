# MCP 2026-07-28 rollout

The `/v1/mcp` endpoint accepts only the `2026-07-28` tools protocol. Every POST needs
`MCP-Protocol-Version: 2026-07-28`, `Mcp-Method`, the request metadata envelope, and the
existing `Token` credential. Tool calls also need `Mcp-Name`. GET and DELETE on `/v1/mcp`, and
the old `/v1/mcp/sse` and `/v1/mcp/messages/:id` routes, are closed.

Requests with an `Origin` header are rejected unless that exact origin is configured in
`stargate.mcp.allowed-origins`. Server-side MCP clients normally send no `Origin` header.

## Release gate

Quarkiverse MCP 2.0.1 incorrectly requires optional `clientInfo` on normal stateless requests.
The migration cannot be released until a stable Quarkiverse version containing the fix is
available, the BOM is pinned to it, and the disabled omission test in `McpProtocolGuardTest`
passes. The local discovery response also mirrors `serverInfo` at the old top-level location for
Quarkiverse 2.0.1's stateless test client; remove that mirror when the test client reads `_meta`.

## Verification

The raw HTTP tests run with `McpProtocolGuardTest`. The existing MCP integration tests use
Quarkiverse's stateless client. Run the database suites with the `dse69-it` and `hcd-it` Maven
profiles in an environment with access to their Testcontainers images.

The independent TypeScript SDK v2 smoke client is in `src/test/mcp-client-smoke`. From that
directory, run `npm ci`, then set `MCP_URL` and `MCP_TOKEN` and run `npm run smoke`. The client
pins `2026-07-28`, discovers the server, lists tools, and calls `findKeyspaces`. For a disposable
staging database, set `MCP_TEST_KEYSPACE=mcp_smoke_<unique_name>` to also create, read, and drop a
keyspace. `MCP_LIST_ONLY=1` restricts the check to discovery and listing when no database is
available.

Enable the existing MCP feature flag for a staging test database before the full smoke test,
then enable production access through that flag. During rollout, watch
`mcp.protocol.rejections`, `mcp.authentication.failures`, `mcp.origin.rejections`, and
`mcp.tool.errors` counters, along with HTTP status and latency for `/v1/mcp`.
A tool failure remains a successful HTTP response with `result.isError: true`;
monitor the counter rather than HTTP 5xx alone.
