package io.stargate.sgv2.jsonapi.api.v1.mcp;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/** Repeats the HTTP regressions with framework diagnostics and raw traffic explicitly enabled. */
@QuarkusTest
@TestProfile(McpDiagnosticLoggingTest.DiagnosticProfile.class)
class McpDiagnosticLoggingTest extends McpLoggingTest {
  public static class DiagnosticProfile extends LoggingProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      var overrides = new HashMap<>(super.getConfigOverrides());
      overrides.put(
          "stargate.jsonapi.mcp.diagnostics-until", Instant.now().plusSeconds(300).toString());
      overrides.put("quarkus.mcp.server.traffic-logging.enabled", "true");
      overrides.put("quarkus.log.category.\"io.quarkiverse.mcp.server\".level", "DEBUG");
      return overrides;
    }
  }
}
