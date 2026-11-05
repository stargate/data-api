package io.stargate.sgv2.jsonapi.api.v1.mcp;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.util.HashMap;
import java.util.Map;

@QuarkusTest
@TestProfile(McpOAuthCustomPathTest.CustomPathProfile.class)
class McpOAuthCustomPathTest extends McpOAuthTest {
  @Override
  protected String expectedResource() {
    return McpOAuthTestSupport.PUBLIC_BASE + "/service/tools";
  }

  @Override
  protected String expectedMetadataUrl() {
    return McpOAuthTestSupport.PUBLIC_BASE + "/service" + McpOAuthTestSupport.METADATA_PATH;
  }

  public static class CustomPathProfile extends OAuthProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      var overrides = new HashMap<>(super.getConfigOverrides());
      overrides.put("quarkus.http.root-path", "/service");
      overrides.put("quarkus.mcp.server.http.root-path", "/tools");
      overrides.put("mp.jwt.verify.audiences", McpOAuthTestSupport.PUBLIC_BASE + "/service/tools");
      return overrides;
    }
  }
}
