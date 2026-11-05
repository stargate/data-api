package io.stargate.sgv2.jsonapi.api.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.jwt.auth.principal.JWTAuthContextInfo;
import io.stargate.sgv2.jsonapi.config.AuthConfig;
import io.stargate.sgv2.jsonapi.config.McpAuthConfig;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.FeaturesConfig;
import io.vertx.ext.web.RoutingContext;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class McpOAuthMetadataTest {
  private static final String PUBLIC_BASE = "https://data.example.test";
  private static final String AUTH_SERVER = "https://auth.example.test/oauth";
  private static final String ENDPOINT = "/v1/mcp";
  private static final String METADATA = ProtectedResourceMetadataResource.PATH;

  @Test
  void serializesConfiguredResourceAndOmitsUnconfiguredScopes() throws Exception {
    var metadata =
        metadata(config(PUBLIC_BASE), ENDPOINT, METADATA, true, true, PUBLIC_BASE + ENDPOINT);
    var mapper = new ObjectMapper();
    JsonNode actual = mapper.valueToTree(metadata.document().orElseThrow());
    assertThat(actual)
        .isEqualTo(
            mapper.readTree(
                """
                {
                  "resource": "https://data.example.test/v1/mcp",
                  "authorization_servers": ["https://auth.example.test/oauth"],
                  "bearer_methods_supported": ["header"]
                }
                """));
  }

  @Test
  void publishesOnlyConfiguredScopes() {
    var config = config(PUBLIC_BASE);
    when(config.scopesSupported()).thenReturn(Optional.of(List.of("db:read", "db:write")));
    assertThat(
            metadata(config, ENDPOINT, METADATA, true, true, PUBLIC_BASE + ENDPOINT)
                .document()
                .orElseThrow()
                .scopesSupported())
        .containsExactly("db:read", "db:write");
  }

  @Test
  void sharesTheEffectiveEndpointAndHttpRootWithChallenges() {
    var metadata =
        metadata(
            config(PUBLIC_BASE),
            "/service/tools",
            "/service" + METADATA,
            true,
            true,
            PUBLIC_BASE + "/service/tools");
    assertThat(metadata.document().orElseThrow().resource())
        .isEqualTo(URI.create(PUBLIC_BASE + "/service/tools"));
    assertThat(metadata.challenge(request("/service/tools")))
        .contains("Bearer resource_metadata=\"" + PUBLIC_BASE + "/service" + METADATA + "\"");
    assertThat(metadata.challenge(request("/service/tools/sse"))).isPresent();
    assertThat(metadata.challenge(request("/service/tools/messages/session-id"))).isPresent();
    assertThat(metadata.challenge(request("/service/tools/collections"))).isEmpty();
    assertThat(metadata.challenge(request("/service/toolsmith"))).isEmpty();
    assertThat(metadata.challenge(request("/v1/collections"))).isEmpty();
  }

  @Test
  void disabledFeaturesDoNotRequireOrAdvertiseAuthorizationConfiguration() {
    var missing = mock(McpAuthConfig.class);
    for (boolean bearerEnabled : List.of(false, true)) {
      var metadata = metadata(missing, ENDPOINT, METADATA, bearerEnabled, false);
      assertThat(metadata.document()).isEmpty();
      assertThat(metadata.challenge(mock(RoutingContext.class))).isEmpty();
    }
    assertThat(metadata(missing, ENDPOINT, METADATA, false, true).document()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {"/", "/service"})
  void oauthRejectsEndpointsAtTheHttpRootEvenWhenMcpIsDisabled(String endpoint) {
    String metadataPath = (endpoint.equals("/") ? "" : endpoint) + METADATA;
    var missing = mock(McpAuthConfig.class);
    for (boolean mcpEnabled : List.of(false, true)) {
      assertThatIllegalArgumentException()
          .isThrownBy(() -> metadata(missing, endpoint, metadataPath, true, mcpEnabled))
          .withMessageContaining("quarkus.mcp.server.http.root-path")
          .withMessageContaining("dedicated path below the HTTP root");
      assertThat(metadata(missing, endpoint, metadataPath, false, mcpEnabled).document()).isEmpty();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"/other/tools", "/service-sibling/tools"})
  void oauthRejectsEndpointsOutsideTheHttpRoot(String endpoint) {
    var missing = mock(McpAuthConfig.class);
    for (boolean mcpEnabled : List.of(false, true)) {
      assertThatIllegalArgumentException()
          .isThrownBy(() -> metadata(missing, endpoint, "/service" + METADATA, true, mcpEnabled))
          .withMessageContaining("below the HTTP root");
    }
    assertThat(metadata(missing, endpoint, "/service" + METADATA, false, true).document())
        .isEmpty();
  }

  @Test
  void identifiesTransportPathsEvenWhenDiscoveryIsDisabled() {
    var metadata = metadata(mock(McpAuthConfig.class), ENDPOINT, METADATA, false, false);
    assertThat(metadata.document()).isEmpty();
    assertThat(metadata.isMcpRequest(request(ENDPOINT))).isTrue();
    assertThat(metadata.isMcpRequest(request("/v1/collections"))).isFalse();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "http://data.example.test",
        "https://data.example.test/path",
        "https://user:data@data.example.test",
        "https://data.example.test?secret=value",
        "https://data.example.test#fragment",
        "/relative"
      })
  void rejectsUntrustedPublicOriginsWithoutEchoingTheirValues(String publicBase) {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> metadata(config(publicBase), ENDPOINT, METADATA, true, true))
        .withMessageContaining("stargate.mcp.auth.public-base-url")
        .withMessageNotContaining(publicBase);
  }

  @ParameterizedTest
  @ValueSource(strings = {"http://localhost:8083", "http://127.0.0.1:8083", "http://[::1]:8083"})
  void acceptsLoopbackHttpForLocalDevelopment(String publicBase) {
    assertThat(
            metadata(config(publicBase), ENDPOINT, METADATA, true, true, publicBase + ENDPOINT)
                .document()
                .orElseThrow()
                .resource())
        .isEqualTo(URI.create(publicBase + ENDPOINT));
  }

  @Test
  void requiresDiscoveryAndAudienceConfigurationWhenEnabled() {
    var missingBase = config(PUBLIC_BASE);
    when(missingBase.publicBaseUrl()).thenReturn(Optional.empty());
    assertThatIllegalArgumentException()
        .isThrownBy(() -> metadata(missingBase, ENDPOINT, METADATA, true, true))
        .withMessageContaining("stargate.mcp.auth.public-base-url is required");
    var missingServer = config(PUBLIC_BASE);
    when(missingServer.serverUrl()).thenReturn(Optional.empty());
    assertThatIllegalArgumentException()
        .isThrownBy(() -> metadata(missingServer, ENDPOINT, METADATA, true, true))
        .withMessageContaining("stargate.mcp.auth.server-url is required");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> metadata(config(PUBLIC_BASE), ENDPOINT, METADATA, true, true))
        .withMessageContaining("mp.jwt.verify.audiences");
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                metadata(
                    config(PUBLIC_BASE),
                    ENDPOINT,
                    METADATA,
                    true,
                    true,
                    "https://other.example.test"))
        .withMessageContaining("mp.jwt.verify.audiences");
  }

  private McpAuthConfig config(String publicBase) {
    var config = mock(McpAuthConfig.class);
    when(config.publicBaseUrl()).thenReturn(Optional.of(URI.create(publicBase)));
    when(config.serverUrl()).thenReturn(Optional.of(URI.create(AUTH_SERVER)));
    when(config.scopesSupported()).thenReturn(Optional.empty());
    return config;
  }

  private McpOAuthMetadata metadata(
      McpAuthConfig config,
      String endpoint,
      String metadataPath,
      boolean bearerEnabled,
      boolean mcpEnabled,
      String... audiences) {
    var auth = mock(AuthConfig.class);
    var bearer = mock(AuthConfig.BearerAuthConfig.class);
    when(auth.bearer()).thenReturn(bearer);
    when(bearer.enabled()).thenReturn(bearerEnabled);
    var features = mock(FeaturesConfig.class);
    when(features.flags()).thenReturn(Map.of(ApiFeature.MCP, Boolean.toString(mcpEnabled)));
    var jwtConfig = new JWTAuthContextInfo();
    jwtConfig.setExpectedAudience(Set.of(audiences));
    return new McpOAuthMetadata(auth, features, config, endpoint, metadataPath, jwtConfig);
  }

  private RoutingContext request(String path) {
    var context = mock(RoutingContext.class);
    when(context.normalizedPath()).thenReturn(path);
    return context;
  }
}
