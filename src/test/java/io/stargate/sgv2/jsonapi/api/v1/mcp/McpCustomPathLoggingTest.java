package io.stargate.sgv2.jsonapi.api.v1.mcp;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.util.HashMap;
import java.util.Map;

@QuarkusTest
@TestProfile(McpCustomPathLoggingTest.CustomPathProfile.class)
class McpCustomPathLoggingTest extends McpLoggingTest {
  public static class CustomPathProfile extends LoggingProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      var overrides = new HashMap<>(super.getConfigOverrides());
      overrides.put("quarkus.http.root-path", "/service");
      overrides.put("quarkus.mcp.server.http.root-path", "/tools");
      overrides.put("quarkus.http.access-log.exclude-pattern", "/service/tools(/.*)?");
      overrides.put(
          "quarkus.http.auth.permission.default.paths", "/service/tools,/service/tools/*");
      return overrides;
    }
  }
}
