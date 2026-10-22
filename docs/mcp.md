# MCP tools

The Data API exposes commands as MCP tools over Streamable HTTP. Use `tools/list` to discover their input schemas. The [tool behavior guide](mcp-tool-behavior-hints.md) explains the catalog hints and retry behavior.

## Endpoint and requests

The default endpoint is `/v1/mcp`. It follows the HTTP and MCP root paths described in the [configuration guide](../CONFIGURATION.md#mcp-logging). Keep authentication paths aligned when changing the endpoint.

Tool execution is disabled by default. Enable it with `stargate.feature.flags.mcp=true`, or send `Feature-Flag-mcp` with value `true` when the server has no configured override. This flag does not grant database permissions.

Current tests send protocol version `2026-07-28` and call tools without initialization or a session ID. Each request carries its own protocol metadata. [McpHttpTestSupport](../src/test/java/io/stargate/sgv2/jsonapi/api/v1/mcp/McpHttpTestSupport.java) defines the tested headers and JSON-RPC envelope.

The current implementation uses the existing Data API `Token` header. A missing token returns HTTP 401 before tool execution. OAuth connection and migration instructions remain separate work in [#2643](https://github.com/stargate/data-api/issues/2643). These tests do not establish compatibility with every MCP client or protocol revision.

## Results and errors

A Data API command result includes readable text containing the full JSON response. This includes any data, status, and errors. Successful structured content contains the data, while status remains in `_meta.status`. For application errors, structured content contains the errors and `isError` is true. Read the text when the complete response is needed, including partial results.

Unknown keyspaces and collections return application errors in a completed tool result. Unexpected setup failures return a generic message. Malformed JSON, unknown methods, and unknown tools remain JSON-RPC errors. Missing required tool arguments remain framework tool errors and can contain plain text without structured content. Authentication failures retain their HTTP handling.

## Command validation

MCP applies the same command validation annotations as HTTP before schema lookup or command execution. Invalid arguments return `COMMAND_FIELD_VALUE_INVALID` as an application error. Validation rules and configurable limits remain on the shared command models.

The [endpoint comparison tests](../src/test/java/io/stargate/sgv2/jsonapi/api/v1/mcp/McpCommandValidationTest.java) cover empty and oversized insert batches, invalid nested find options, and skip without sort. Valid inputs at the batch limit and omitted, null, or empty optional arguments remain accepted.

Use the schemas returned by `tools/list` for accepted arguments. The [query specification](dataapi-spec.md) describes command semantics, and the [configuration guide](../CONFIGURATION.md#operations-configuration) records configurable operation limits.

## Request isolation and tracing

Send `Feature-Flag-request-tracing` with value `true` to include diagnostic events for a command. Each call has its own request context, generated request ID, and diagnostic trace. The [overlapping-call tests](../src/test/java/io/stargate/sgv2/jsonapi/api/v1/mcp/McpLoggingTest.java) check that callbacks and returned events stay with their original call. The [subscription cancellation test](../src/test/java/io/stargate/sgv2/jsonapi/api/v1/mcp/McpResourceTest.java) checks cleanup while another command remains active.

These diagnostic events are separate from distributed tracing. The current build does not include the OpenTelemetry extension. It does not import `traceparent`, `tracestate`, or `baggage` from request `_meta` or HTTP headers into a distributed trace. Those fields do not supply authentication or the generated request IDs.

Quarkiverse MCP 2.0.3 has a [native metadata instrumenter](https://github.com/quarkiverse/quarkus-mcp-server/blob/2.0.3/core/runtime/src/main/java/io/quarkiverse/mcp/server/runtime/tracing/McpTracingInstrumenter.java), which is [registered only when OpenTelemetry tracing is enabled](https://github.com/quarkiverse/quarkus-mcp-server/blob/2.0.3/core/deployment/src/main/java/io/quarkiverse/mcp/server/deployment/McpServerProcessor.java). Enabling that integration requires allowed baggage keys, size limits, and interleaving and cancellation checks for distributed traces. That work remains tracked in [#2629](https://github.com/stargate/data-api/issues/2629).

## Diagnostics

Follow [MCP logging](../CONFIGURATION.md#mcp-logging) for request correlation, redaction, and temporary diagnostics. That section also covers custom endpoint paths and log handlers.
