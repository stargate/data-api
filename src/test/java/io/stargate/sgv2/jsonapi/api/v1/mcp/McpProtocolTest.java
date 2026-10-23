package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.assertResponseComplete;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.request;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.rpcPayload;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Exercises the HTTP wire contract without a client that might initialize a session for us. */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class McpProtocolTest {
  @Test
  void listsToolsWithoutInitializationOrSession() {
    // Each POST is independent. clientInfo, discovery and initialization are deliberately absent.
    for (int id = 1; id <= 2; id++) {
      var response =
          request("tools/list")
              // Header auth checks only that a token is present. Database requests validate it,
              // but tools/list returns metadata without accessing the database.
              .header("Token", "protocol-test-credential")
              .body(rpcPayload(id, "tools/list", Map.of()))
              .post("/v1/mcp");
      assertResponseComplete(response, id);
      assertThat(response.header("Mcp-Session-Id")).isNull();
      assertThat(response.jsonPath().getList("result.tools.name")).isNotEmpty();
    }
  }
}
