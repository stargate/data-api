package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.assertResponseComplete;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.request;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.rpcPayload;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpOAuthTestSupport.AUTH_SERVER;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpOAuthTestSupport.FIRST_ORG;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpOAuthTestSupport.FIRST_SUBJECT;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpOAuthTestSupport.METADATA_PATH;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpOAuthTestSupport.token;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verifyNoInteractions;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import io.stargate.sgv2.jsonapi.service.processor.CommandProcessor;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/** One deployment that serves a separate public host, and so a separate resource, per database. */
@QuarkusTest
@TestProfile(McpOAuthHostSuffixTest.HostSuffixProfile.class)
class McpOAuthHostSuffixTest {
  private static final String HOST_SUFFIX = ".apps.example.test";
  private static final String FIRST_HOST = "db-one-us-east-2" + HOST_SUFFIX;
  private static final String SECOND_HOST = "db-two-us-east-2" + HOST_SUFFIX;
  private static final String MCP_PATH = "/v1/mcp";

  @InjectMock CommandProcessor commandProcessor;

  @ParameterizedTest
  @ValueSource(strings = {FIRST_HOST, SECOND_HOST})
  void discoveryNamesTheRequestedDatabaseHost(String host) {
    var metadata =
        given().header("Host", host).get(METADATA_PATH).then().statusCode(200).extract().jsonPath();

    assertThat(metadata.getString("resource")).isEqualTo(resource(host));
    assertThat(metadata.getList("authorization_servers", String.class))
        .containsExactly(AUTH_SERVER);
  }

  @Test
  void discoveryPublishesTheCanonicalFormOfTheRequestHost() {
    var metadata =
        given()
            .header("Host", FIRST_HOST.toUpperCase(Locale.ROOT) + ":443")
            .get(METADATA_PATH)
            .then()
            .statusCode(200)
            .extract()
            .jsonPath();

    assertThat(metadata.getString("resource")).isEqualTo(resource(FIRST_HOST));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "data.attacker.test",
        "db-one-us-east-2.apps.example.test.",
        "db-one.other.apps.example.test"
      })
  void discoveryIsAbsentOnAHostTheDeploymentDoesNotServe(String host) {
    var response = given().header("Host", host).get(METADATA_PATH);

    assertThat(response.statusCode()).as(response.asString()).isEqualTo(404);
    assertThat(response.asString()).doesNotContain(host);
  }

  @Test
  void unauthenticatedRequestsAreChallengedOnTheirOwnHost() {
    assertChallenge(catalogRequest(FIRST_HOST).post(MCP_PATH), FIRST_HOST);
    assertChallenge(catalogRequest(SECOND_HOST).post(MCP_PATH), SECOND_HOST);
  }

  @Test
  void aTokenIssuedForTheRequestedDatabaseListsTools() {
    var response =
        catalogRequest(FIRST_HOST)
            .header("Authorization", "Bearer " + tokenFor(resource(FIRST_HOST)))
            .post(MCP_PATH);

    assertResponseComplete(response, 1);
    assertThat(response.jsonPath().getList("result.tools.name", String.class)).contains("findOne");
  }

  @Test
  void aTokenIssuedForAnotherDatabaseCannotReachTheTool() {
    var response =
        providerRequest(FIRST_HOST)
            .header("Authorization", "Bearer " + tokenFor(resource(SECOND_HOST)))
            .post(MCP_PATH);

    assertChallenge(response, FIRST_HOST);
    verifyNoInteractions(commandProcessor);
  }

  @Test
  void aTokenIssuedForTheHostWithoutTheMcpPathCannotReachTheTool() {
    var response =
        providerRequest(FIRST_HOST)
            .header("Authorization", "Bearer " + tokenFor("https://" + FIRST_HOST))
            .post(MCP_PATH);

    assertChallenge(response, FIRST_HOST);
    verifyNoInteractions(commandProcessor);
  }

  @ParameterizedTest
  @EnumSource(
      value = McpOAuthTestSupport.InvalidToken.class,
      names = {"WRONG_AUDIENCE", "MISSING_AUDIENCE"})
  void tokensWithoutTheRequestedAudienceCannotReachTheTool(McpOAuthTestSupport.InvalidToken kind) {
    var response =
        providerRequest(FIRST_HOST)
            .header(
                "Authorization",
                "Bearer " + McpOAuthTestSupport.invalidToken(kind, resource(FIRST_HOST)))
            .post(MCP_PATH);

    assertChallenge(response, FIRST_HOST);
    verifyNoInteractions(commandProcessor);
  }

  @Test
  void aTokenNamingAnUnservedHostIsRejectedWithoutAChallenge() {
    // A token another service received for its own audience must not work here, even when the
    // request claims that service's host.
    String host = "data.attacker.test";
    var response =
        providerRequest(host)
            .header("Authorization", "Bearer " + tokenFor(resource(host)))
            .post(MCP_PATH);

    assertThat(response.statusCode()).as(response.asString()).isEqualTo(401);
    assertThat(response.header("WWW-Authenticate")).isNull();
    assertThat(response.asString()).doesNotContain(host);
    verifyNoInteractions(commandProcessor);
  }

  private static String resource(String host) {
    return "https://" + host + MCP_PATH;
  }

  private static String tokenFor(String audience) {
    return token(FIRST_SUBJECT, FIRST_ORG, "read-role", audience);
  }

  private static RequestSpecification catalogRequest(String host) {
    return request("tools/list").header("Host", host).body(rpcPayload(1, "tools/list", Map.of()));
  }

  private static RequestSpecification providerRequest(String host) {
    return request("tools/call")
        .header("Host", host)
        .header("Mcp-Name", "findEmbeddingProviders")
        .body(McpHttpTestSupport.toolPayload(1, "findEmbeddingProviders", Map.of()));
  }

  private static void assertChallenge(Response response, String host) {
    assertThat(response.statusCode()).as(response.asString()).isEqualTo(401);
    assertThat(response.header("WWW-Authenticate"))
        .as("HTTP headers: %s; body: %s", response.headers(), response.asString())
        .contains("Bearer", "resource_metadata=\"https://" + host + METADATA_PATH + "\"");
  }

  public static class HostSuffixProfile implements NoGlobalResourcesTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      // mp.jwt.verify.audiences is left unset because each host is its own audience.
      return Map.of(
          "stargate.feature.flags.mcp",
          "true",
          "stargate.auth.bearer.enabled",
          "true",
          "stargate.mcp.auth.server-url",
          AUTH_SERVER,
          "stargate.mcp.auth.public-host-suffix",
          HOST_SUFFIX,
          "mp.jwt.verify.issuer",
          AUTH_SERVER,
          "mp.jwt.verify.publickey.location",
          "mcp-oauth-public-key.pem");
    }
  }
}
