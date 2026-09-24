import assert from 'node:assert/strict';
import { Client, StreamableHTTPClientTransport } from '@modelcontextprotocol/client';

const url = new URL(process.env.MCP_URL ?? 'http://localhost:8080/v1/mcp');
const token = process.env.MCP_TOKEN;
if (!token) throw new Error('Set MCP_TOKEN to a Token credential for a test database');

const client = new Client(
  { name: 'data-api-mcp-smoke', version: '1.0.0' },
  { versionNegotiation: { mode: { pin: '2026-07-28' } } },
);

function envelope(result) {
  assert.equal(result.isError, false, JSON.stringify(result));
  assert.ok(result.content?.length, 'Tool result must contain text content');
  assert.equal(result.content[0].type, 'text');
  return JSON.parse(result.content[0].text);
}

try {
  await client.connect(
    new StreamableHTTPClientTransport(url, { requestInit: { headers: { Token: token } } }),
  );
  assert.equal(client.getProtocolEra(), 'modern');
  assert.deepEqual(client.getDiscoverResult()?.supportedVersions, ['2026-07-28']);

  const list = await client.listTools();
  assert.ok(list.tools.some(tool => tool.name === 'findKeyspaces'));
  assert.ok(list.tools.some(tool => tool.name === 'createKeyspace'));

  if (process.env.MCP_LIST_ONLY !== '1') {
    const read = envelope(await client.callTool({ name: 'findKeyspaces', arguments: {} }));
    assert.ok(read.status || read.data);
  }

  const keyspace = process.env.MCP_TEST_KEYSPACE;
  if (keyspace && process.env.MCP_LIST_ONLY !== '1') {
    if (!/^mcp_smoke_[a-z0-9_]+$/.test(keyspace)) {
      throw new Error('MCP_TEST_KEYSPACE must be a disposable mcp_smoke_ name');
    }
    envelope(await client.callTool({ name: 'createKeyspace', arguments: { name: keyspace } }));
    try {
      const afterCreate = envelope(await client.callTool({ name: 'findKeyspaces', arguments: {} }));
      assert.ok(JSON.stringify(afterCreate).includes(keyspace));
    } finally {
      envelope(await client.callTool({ name: 'dropKeyspace', arguments: { name: keyspace } }));
    }
  }

  process.stdout.write('MCP 2026-07-28 SDK smoke passed\n');
} finally {
  await client.close();
}
