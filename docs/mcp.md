# MCP tools

The Data API exposes commands as MCP tools over Streamable HTTP. Use `tools/list` to discover their input schemas. The [tool behavior guide](mcp-tool-behavior-hints.md) explains the catalog hints and retry behavior.

## Endpoint and requests

The default endpoint is `/v1/mcp`. It follows the HTTP and MCP root paths described in the [configuration guide](../CONFIGURATION.md#mcp-logging). The server derives the MCP authentication paths from that configuration.

Tool execution is disabled by default. Enable it with `stargate.feature.flags.mcp=true`, or send `Feature-Flag-mcp` with value `true` when the server has no configured override. This flag does not grant database permissions.

Current tests send protocol version `2026-07-28` and call tools without initialization or a session ID. Each request carries its own protocol metadata. [McpHttpTestSupport](../src/test/java/io/stargate/sgv2/jsonapi/api/v1/mcp/McpHttpTestSupport.java) defines the tested headers and JSON-RPC envelope.

The existing Data API `Token` and `X-Cassandra-Token` headers remain supported on ordinary REST endpoints when header authentication is enabled. MCP retains that behavior until OAuth mode is enabled. Missing credentials return HTTP 401 before tool execution. These tests do not establish compatibility with every MCP client or protocol revision.

## OAuth authentication

OAuth mode is disabled by default. Enabling it makes the MCP transport accept only Bearer access tokens and leaves ordinary REST authentication unchanged. Configure the public MCP origin, authorization server, and JWT trust settings. For the default endpoint:

```properties
stargate.auth.bearer.enabled=true
stargate.feature.flags.mcp=true
stargate.mcp.auth.public-base-url=https://api.example.com
stargate.mcp.auth.server-url=https://auth.example.com
mp.jwt.verify.issuer=https://auth.example.com
mp.jwt.verify.publickey.location=https://auth.example.com/.well-known/jwks.json
mp.jwt.verify.audiences=https://api.example.com/v1/mcp
```

Use the issuer and verification key published by your authorization server. A local public-key file can replace the JWKS URL. Bearer authentication requires an explicit issuer, verification key, and audience at startup. With MCP enabled, the audience must include the public MCP resource URL.

The public base URL contains the scheme, host, and optional port. The server derives the MCP resource path from its HTTP and MCP root configuration. For example, HTTP root `/service` and MCP root `/tools` require audience `https://api.example.com/service/tools`. OAuth mode requires a dedicated MCP path beneath the HTTP root. Root-only configurations are rejected at startup because the current framework does not keep those transport routes within application authentication. Public discovery requires MCP to be enabled in server configuration. Its metadata is available without credentials at `/.well-known/oauth-protected-resource`, under the HTTP root when configured, and is absent when Bearer authentication or MCP is disabled.

Unauthenticated MCP requests receive HTTP 401 with an absolute `WWW-Authenticate: Bearer resource_metadata="..."` URL. Send `Authorization: Bearer <access-token>` on every subsequent HTTP request, including initialization for older protocols and catalog requests. The validator checks the token before tool execution. Legacy MCP headers and invalid Bearer credentials cannot fall back to legacy authentication in OAuth mode. When OAuth mode is disabled, legacy MCP authentication keeps its existing precedence. An MCP access token alone does not authenticate an ordinary REST request.

REST and MCP resolve the same authenticated credential. A JWT's encoded token is retained separately from its subject or client ID. Tool names, arguments, and result formats do not change. Optional `stargate.mcp.auth.scopes-supported` values describe scopes supported by the deployment; advertising them does not grant database permissions.

This endpoint is an OAuth protected resource, not an authorization server. Browser login, client registration, organization/role selection, and token issuance remain outside Data API. The backend must support the agreed JWT credential contract. Successful initialization or tool discovery does not prove authorized database execution.

Local verification used DSE 6.9.21's native OIDC authentication alongside internal password authentication. It required an OIDC issuer, the MCP resource audience, `allow_token_via_password_authentication=true`, and `token_username=token`. DSE mapped an issuer-signed claim to a restricted database role. The issuer derived that claim from validated organization membership and role selection. Internal password authentication alone cannot validate these JWT credentials. Each deployment needs matching issuer, audience, and database permission configuration.

The compatibility tests inspect the HTTP-to-command boundary with a processor test double. Validate real permitted and denied database calls against the deployment's backend as well. Migration work remains tracked in [#2643](https://github.com/stargate/data-api/issues/2643).

### Reusable OAuth smoke test

With OAuth and MCP enabled, use an existing collection and a known document to test the database path. Run from the repository root with Python 3.10 or later.

```sh
python3 docs/mcp/oauth_smoke.py \
  --endpoint https://api.example.com/v1/mcp \
  --keyspace demo_readonly --collection messages \
  --filter '{"id":"welcome"}' \
  --expect-document '{"id":"welcome","value":"hello"}' \
  --denied-keyspace demo_other_org
```

The [script](mcp/oauth_smoke.py) prompts for an access token without echoing it. Automation can supply `MCP_ACCESS_TOKEN` through the environment. Obtain the token from the deployment's authorization server for the exact MCP resource audience. The script consumes that token and does not perform browser login or token issuance.

It checks the unauthenticated HTTP 401 challenge, public resource metadata, authenticated tool catalog, and exact `findOne` document. It prints the available tool names and exits nonzero on HTTP, protocol, application, or document mismatches. The optional `--denied-keyspace` repeats the read against another existing keyspace and requires `UNAUTHORIZED_ACCESS`. No write tools are called. The script uses the stateless protocol version described above and does not test older session-based clients.

Run the script's focused tests with `python3 -B -m unittest discover -s docs/mcp -p 'test_*.py' -v`.

## Results and errors

A Data API command result keeps text content empty. Successful structured content contains the data, while status remains in `_meta.status`. For application errors, structured content contains the errors and `isError` is true. When a command partly fails, structured content holds only the errors, and status such as inserted IDs stays in `_meta.status`.

Unknown keyspaces and collections return application errors in a completed tool result. Unexpected setup failures return a generic message. Malformed JSON, unknown methods, and unknown tools remain JSON-RPC errors. Missing required tool arguments remain framework tool errors and can contain plain text without structured content. Authentication failures retain their HTTP handling.

## Command validation

MCP applies the same command validation annotations as HTTP before schema lookup or command execution. Invalid arguments return `COMMAND_FIELD_VALUE_INVALID` as an application error. Validation rules and configurable limits remain on the shared command models.

The [endpoint comparison tests](../src/test/java/io/stargate/sgv2/jsonapi/api/v1/mcp/McpCommandValidationTest.java) cover empty and oversized insert batches, invalid nested find options, and skip without sort. Valid inputs at the batch limit and omitted, null, or empty optional arguments remain accepted.

Use the schemas returned by `tools/list` for accepted arguments. The [query specification](dataapi-spec.md) describes command semantics, and the [configuration guide](../CONFIGURATION.md#operations-configuration) records configurable operation limits.

## Request isolation and tracing

Send `Feature-Flag-request-tracing` with value `true` to include diagnostic events for a command. Each call has its own request context, generated request ID, and diagnostic trace. The [overlapping-call tests](../src/test/java/io/stargate/sgv2/jsonapi/api/v1/mcp/McpLoggingTest.java) check that callbacks and returned events stay with their original call. The [subscription cancellation test](../src/test/java/io/stargate/sgv2/jsonapi/api/v1/mcp/McpResourceTest.java) checks cleanup while another command remains active.

These diagnostic events are separate from distributed tracing. The current build does not include the OpenTelemetry extension. It does not import `traceparent`, `tracestate`, or `baggage` from request `_meta` or HTTP headers into a distributed trace. Those fields do not supply authentication or the generated request IDs.

Enabling distributed tracing and defining allowed baggage keys and size limits remain tracked in [#2629](https://github.com/stargate/data-api/issues/2629).

## Diagnostics

Follow [MCP logging](../CONFIGURATION.md#mcp-logging) for request correlation, redaction, and temporary diagnostics. That section also covers custom endpoint paths and log handlers.
