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
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.HostAndPort;
import io.vertx.ext.web.RoutingContext;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class McpOAuthMetadataTest {
  private static final String PUBLIC_BASE = "https://data.example.test";
  private static final String AUTH_SERVER = "https://auth.example.test/oauth";
  private static final String ENDPOINT = "/v1/mcp";
  private static final String METADATA = ProtectedResourceMetadataResource.PATH;
  private static final String HOST_SUFFIX = ".apps.example.test";
  private static final String DATABASE_HOST = "db-one-us-east-2.apps.example.test";
  private static final String DATABASE_RESOURCE = "https://" + DATABASE_HOST + ENDPOINT;

  @Test
  void serializesConfiguredResourceAndOmitsUnconfiguredScopes() throws Exception {
    var metadata =
        metadata(config(PUBLIC_BASE), ENDPOINT, METADATA, true, true, PUBLIC_BASE + ENDPOINT);
    var mapper = new ObjectMapper();
    JsonNode actual = mapper.valueToTree(metadata.document(request(ENDPOINT)).orElseThrow());
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
                .document(request(ENDPOINT))
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
    assertThat(metadata.document(request(ENDPOINT)).orElseThrow().resource())
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
      assertThat(metadata.document(request(ENDPOINT))).isEmpty();
      assertThat(metadata.challenge(mock(RoutingContext.class))).isEmpty();
    }
    assertThat(metadata(missing, ENDPOINT, METADATA, false, true).document(request(ENDPOINT)))
        .isEmpty();
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
      assertThat(
              metadata(missing, endpoint, metadataPath, false, mcpEnabled)
                  .document(request(ENDPOINT)))
          .isEmpty();
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
    assertThat(
            metadata(missing, endpoint, "/service" + METADATA, false, true)
                .document(request(ENDPOINT)))
        .isEmpty();
  }

  @Test
  void identifiesTransportPathsEvenWhenDiscoveryIsDisabled() {
    var metadata = metadata(mock(McpAuthConfig.class), ENDPOINT, METADATA, false, false);
    assertThat(metadata.document(request(ENDPOINT))).isEmpty();
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
                .document(request(ENDPOINT))
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
        .withMessageContaining(
            "stargate.mcp.auth.public-base-url or stargate.mcp.auth.public-host-suffix is required");
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

  @Test
  void hostSuffixPublishesEachRequestHostAsItsOwnResource() throws Exception {
    var metadata = metadata(hostConfig(HOST_SUFFIX), ENDPOINT, METADATA, true, true);
    var mapper = new ObjectMapper();
    JsonNode actual =
        mapper.valueToTree(metadata.document(request(METADATA, DATABASE_HOST, -1)).orElseThrow());
    assertThat(actual)
        .isEqualTo(
            mapper.readTree(
                """
                {
                  "resource": "https://db-one-us-east-2.apps.example.test/v1/mcp",
                  "authorization_servers": ["https://auth.example.test/oauth"],
                  "bearer_methods_supported": ["header"]
                }
                """));
    assertThat(metadata.challenge(request(ENDPOINT, DATABASE_HOST, -1)))
        .contains("Bearer resource_metadata=\"https://" + DATABASE_HOST + METADATA + "\"");
    assertThat(
            metadata
                .document(request(METADATA, "db-two-eu-west-1.apps.example.test", -1))
                .orElseThrow()
                .resource())
        .isEqualTo(URI.create("https://db-two-eu-west-1.apps.example.test/v1/mcp"));
  }

  @Test
  void hostSuffixUsesTheCanonicalHostAndKeepsANonDefaultPort() {
    var metadata = metadata(hostConfig(HOST_SUFFIX), ENDPOINT, METADATA, true, true);
    assertThat(
            metadata
                .document(request(METADATA, "DB-One-US-East-2.Apps.Example.Test", 443))
                .orElseThrow()
                .resource())
        .isEqualTo(URI.create(DATABASE_RESOURCE));
    assertThat(metadata.document(request(METADATA, DATABASE_HOST, 8443)).orElseThrow().resource())
        .isEqualTo(URI.create("https://" + DATABASE_HOST + ":8443" + ENDPOINT));
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(
      strings = {
        "data.example.test",
        "apps.example.test",
        ".apps.example.test",
        "db-one.other.apps.example.test",
        "db-one-us-east-2.apps.example.test.attacker.test",
        "db-one-us-east-2.apps.example.test.",
        "-db-one.apps.example.test",
        "db-one-.apps.example.test",
        "db_one.apps.example.test",
        "db-one-us-east-2xapps.example.test"
      })
  void hostSuffixRejectsHostsItDoesNotServe(String host) {
    var metadata = metadata(hostConfig(HOST_SUFFIX), ENDPOINT, METADATA, true, true);
    assertThat(metadata.document(request(METADATA, host, -1))).isEmpty();
    assertThat(metadata.challenge(request(ENDPOINT, host, -1))).isEmpty();
    assertThat(
            metadata.acceptsAudience(
                request(ENDPOINT, host, -1), Set.of("https://" + host + ENDPOINT)))
        .isFalse();
  }

  @Test
  void hostSuffixAcceptsOnlyTokensIssuedForTheRequestedDatabase() {
    var metadata = metadata(hostConfig(HOST_SUFFIX), ENDPOINT, METADATA, true, true);
    var request = request(ENDPOINT, DATABASE_HOST, -1);
    assertThat(metadata.audienceFromRequestHost()).isTrue();
    assertThat(metadata.acceptsAudience(request, Set.of(DATABASE_RESOURCE))).isTrue();
    assertThat(metadata.acceptsAudience(request, Set.of("another-audience", DATABASE_RESOURCE)))
        .isTrue();
    assertThat(
            metadata.acceptsAudience(
                request, Set.of("https://db-two-eu-west-1.apps.example.test/v1/mcp")))
        .isFalse();
    // The audience names the MCP endpoint, not only the database host.
    assertThat(metadata.acceptsAudience(request, Set.of("https://" + DATABASE_HOST))).isFalse();
    assertThat(metadata.acceptsAudience(request, Set.of())).isFalse();
    assertThat(metadata.acceptsAudience(request, null)).isFalse();
  }

  @Test
  void hostSuffixChecksTheAudienceWhenMcpIsNotEnabledInServerConfiguration() {
    // A request header can still enable MCP, so the audience rule cannot depend on discovery.
    var metadata = metadata(hostConfig(HOST_SUFFIX), ENDPOINT, METADATA, true, false);
    var request = request(ENDPOINT, DATABASE_HOST, -1);
    assertThat(metadata.document(request)).isEmpty();
    assertThat(metadata.challenge(request)).isEmpty();
    assertThat(metadata.acceptsAudience(request, Set.of(DATABASE_RESOURCE))).isTrue();
    assertThat(metadata.acceptsAudience(request, Set.of("https://other.example.test/v1/mcp")))
        .isFalse();
  }

  @Test
  void fixedPublicBaseLeavesTheAudienceToTheConfiguredJwtVerifier() {
    var metadata =
        metadata(config(PUBLIC_BASE), ENDPOINT, METADATA, true, true, PUBLIC_BASE + ENDPOINT);
    assertThat(metadata.audienceFromRequestHost()).isFalse();
    assertThat(metadata.acceptsAudience(request(ENDPOINT), Set.of(PUBLIC_BASE + ENDPOINT)))
        .isTrue();
    assertThat(metadata.acceptsAudience(request(ENDPOINT), null)).isTrue();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "apps.example.test",
        "..",
        ".test",
        ".apps.example.test.",
        ".Apps.Example.Test",
        "*.apps.example.test",
        ".apps..example.test",
        ".apps.example.test:8443",
        ".apps.example.test/path",
        "https://.apps.example.test",
        ".-apps.example.test"
      })
  void rejectsMalformedHostSuffixesWithoutEchoingTheirValues(String suffix) {
    for (boolean mcpEnabled : List.of(false, true)) {
      assertThatIllegalArgumentException()
          .isThrownBy(() -> metadata(hostConfig(suffix), ENDPOINT, METADATA, true, mcpEnabled))
          .withMessageContaining("stargate.mcp.auth.public-host-suffix")
          .withMessageNotContaining(suffix);
    }
  }

  @Test
  void rejectsAFixedPublicBaseTogetherWithAHostSuffix() {
    var both = config(PUBLIC_BASE);
    when(both.publicHostSuffix()).thenReturn(Optional.of(HOST_SUFFIX));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> metadata(both, ENDPOINT, METADATA, true, true, PUBLIC_BASE + ENDPOINT))
        .withMessageContaining("stargate.mcp.auth.public-base-url")
        .withMessageContaining("stargate.mcp.auth.public-host-suffix");
  }

  @Test
  void hostSuffixStillRequiresAnAuthorizationServer() {
    var missingServer = hostConfig(HOST_SUFFIX);
    when(missingServer.serverUrl()).thenReturn(Optional.empty());
    assertThatIllegalArgumentException()
        .isThrownBy(() -> metadata(missingServer, ENDPOINT, METADATA, true, true))
        .withMessageContaining("stargate.mcp.auth.server-url is required");
  }

  private McpAuthConfig config(String publicBase) {
    var config = mock(McpAuthConfig.class);
    when(config.publicBaseUrl()).thenReturn(Optional.of(URI.create(publicBase)));
    when(config.serverUrl()).thenReturn(Optional.of(URI.create(AUTH_SERVER)));
    when(config.scopesSupported()).thenReturn(Optional.empty());
    return config;
  }

  private McpAuthConfig hostConfig(String hostSuffix) {
    var config = mock(McpAuthConfig.class);
    when(config.publicBaseUrl()).thenReturn(Optional.empty());
    when(config.publicHostSuffix()).thenReturn(Optional.of(hostSuffix));
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

  /** A request whose Host header is {@code host}, or one without the header when it is null. */
  private RoutingContext request(String path, String host, int port) {
    var context = request(path);
    var request = mock(HttpServerRequest.class);
    when(context.request()).thenReturn(request);
    when(request.authority()).thenReturn(host == null ? null : HostAndPort.create(host, port));
    return context;
  }
}
