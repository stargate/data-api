package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.assertResponseComplete;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.request;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.rpcPayload;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import io.vertx.core.json.JsonObject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Exercises the browser Origin policy at the MCP endpoint with an approved Origin configured. */
@QuarkusTest
@TestProfile(McpOriginPolicyTest.Profile.class)
class McpOriginPolicyTest {
  static final String APPROVED = "https://client.example.com";
  static final String LOCAL_APPROVED = "http://localhost:6274";
  // Header auth checks only that a token is present. tools/list does not access the database.
  static final String TOKEN = "origin-test-credential";

  public static class Profile implements NoGlobalResourcesTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(McpOriginFilter.ALLOWED_ORIGINS, APPROVED + "," + LOCAL_APPROVED);
    }
  }

  @Test
  void allowsNativeClientsWithoutOrigin() {
    var response = listTools(null);
    assertResponseComplete(response, 1);
    assertThat(response.header("Access-Control-Allow-Origin")).isNull();
    assertThat(response.headers().getValues("Vary")).contains("Origin");
  }

  @ParameterizedTest
  @ValueSource(strings = {APPROVED, LOCAL_APPROVED})
  void allowsApprovedOrigins(String origin) {
    var response = listTools(origin);
    assertResponseComplete(response, 1);
    assertThat(response.header("Access-Control-Allow-Origin")).isEqualTo(origin);
    assertThat(response.header("Access-Control-Expose-Headers")).isEqualTo("WWW-Authenticate");
    assertThat(response.header("Access-Control-Allow-Credentials")).isNull();
    assertThat(response.headers().getValues("Vary")).contains("Origin");
  }

  @Test
  void approvedOriginStillRequiresCredentials() {
    var response =
        request("tools/list")
            .header("Origin", APPROVED)
            .body(rpcPayload(1, "tools/list", Map.of()))
            .post("/v1/mcp");
    assertThat(response.statusCode()).isEqualTo(401);
    // The browser can read the 401. WWW-Authenticate is exposed now so the OAuth challenge from
    // #2642 is readable once it is added.
    assertThat(response.header("Access-Control-Allow-Origin")).isEqualTo(APPROVED);
    assertThat(response.header("Access-Control-Expose-Headers")).isEqualTo("WWW-Authenticate");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "https://client.example.com.evil.example",
        "https://evilclient.example.com",
        "https://evil.example",
        "http://client.example.com",
        "https://client.example.com:8443",
        "https://client.example.com:443",
        "https://client.example.com/",
        "https://client.example.com/path",
        "https://CLIENT.example.com",
        "https://user@client.example.com",
        "https://client.example.com, https://evil.example",
        "http://localhost",
        "http://127.0.0.1:6274",
        "null",
        "*",
        "not an origin",
        ""
      })
  void rejectsUnapprovedAndMalformedOrigins(String origin) {
    assertRejected(listTools(origin));
  }

  @Test
  void rejectsMoreThanOneOriginHeader() {
    assertRejected(
        request("tools/list")
            .header("Token", TOKEN)
            .header("Origin", APPROVED)
            .header("Origin", "https://evil.example")
            .body(rpcPayload(1, "tools/list", Map.of()))
            .post("/v1/mcp"));
  }

  @Test
  void rejectsBeforeAuthentication() {
    // 403 rather than 401 shows that authentication did not run for the rejected request.
    assertRejected(
        request("tools/list")
            .header("Origin", "https://evil.example")
            .body(rpcPayload(1, "tools/list", Map.of()))
            .post("/v1/mcp"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"GET", "DELETE"})
  void rejectsOtherMethods(String method) {
    assertRejected(
        request("tools/list")
            .header("Token", TOKEN)
            .header("Origin", "https://evil.example")
            .request(method, "/v1/mcp"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"/v1/mcp", "/v1/mcp/"})
  void answersPreflightFromApprovedOriginWithoutCredentials(String path) {
    var response = preflight(APPROVED, path);
    assertThat(response.statusCode()).isEqualTo(204);
    assertThat(response.header("Access-Control-Allow-Origin")).isEqualTo(APPROVED);
    assertThat(response.header("Access-Control-Allow-Methods")).isEqualTo("POST");
    assertThat(response.header("Access-Control-Allow-Headers"))
        .isEqualTo("Authorization, Content-Type, MCP-Protocol-Version, Mcp-Method, Mcp-Name");
    assertThat(response.header("Access-Control-Allow-Credentials")).isNull();
    assertThat(response.headers().getValues("Vary")).contains("Origin");
    assertThat(response.asString()).isEmpty();
  }

  @Test
  void rejectsPreflightFromUnapprovedOrigin() {
    assertRejected(preflight("https://client.example.com.evil.example", "/v1/mcp"));
  }

  @Test
  void treatsOptionsWithoutRequestMethodAsAnOrdinaryRequest() {
    // Not a CORS preflight, so it continues to authentication instead of getting a 204.
    var response = given().header("Origin", APPROVED).options("/v1/mcp");
    assertThat(response.statusCode()).isEqualTo(401);
    assertThat(response.header("Access-Control-Allow-Origin")).isEqualTo(APPROVED);
    assertThat(response.header("Access-Control-Allow-Methods")).isNull();
  }

  @ParameterizedTest
  // Vert.x routes on the normalized path, so the filter must match these the same way. The JDK
  // client sends the path exactly as written, unlike RestAssured.
  @ValueSource(strings = {"/v1/mcp/", "/v1/%6Dcp", "/v1/./mcp", "/v1/x/../mcp", "/v1/mcp/sse"})
  void rejectsEveryPathThatReachesTheEndpoint(String path) throws Exception {
    var request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + RestAssured.port + path))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", "2026-07-28")
            .header("Mcp-Method", "tools/list")
            .header("Token", TOKEN)
            .header("Origin", "https://evil.example")
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    new JsonObject(rpcPayload(1, "tools/list", Map.of())).encode()))
            .build();
    try (var client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
      var response = client.send(request, HttpResponse.BodyHandlers.ofString());
      assertThat(response.statusCode()).as(path).isEqualTo(403);
      assertThat(response.body()).as(path).isEqualTo(McpOriginFilter.REJECTION_BODY);
    }
  }

  @Test
  void leavesTheOrdinaryHttpApiAlone() {
    // The ordinary HTTP API keeps its own policy. These reach authentication and fail there.
    for (String path : new String[] {"/v1", "/v1/mcpkeyspace"}) {
      var response =
          given()
              .contentType("application/json")
              .header("Origin", "https://evil.example")
              .body(Map.of("findKeyspaces", Map.of()))
              .post(path);
      assertThat(response.statusCode()).as(path).isEqualTo(401);
      assertThat(response.header("Access-Control-Allow-Origin")).as(path).isNull();
    }
  }

  private static Response listTools(String origin) {
    var spec = request("tools/list").header("Token", TOKEN);
    if (origin != null) {
      spec = spec.header("Origin", origin);
    }
    return spec.body(rpcPayload(1, "tools/list", Map.of())).post("/v1/mcp");
  }

  private static Response preflight(String origin, String path) {
    return given()
        .header("Origin", origin)
        .header("Access-Control-Request-Method", "POST")
        .header(
            "Access-Control-Request-Headers",
            "authorization, content-type, mcp-method, mcp-name, mcp-protocol-version")
        .options(path);
  }

  static void assertRejected(Response response) {
    assertThat(response.statusCode()).as(response.asString()).isEqualTo(403);
    assertThat(response.contentType()).startsWith("application/json");
    // A fixed body, so neither the request payload nor its credentials can appear in it.
    assertThat(response.asString()).isEqualTo(McpOriginFilter.REJECTION_BODY);
    assertThat(response.header("Access-Control-Allow-Origin")).isNull();
    assertThat(response.header("Access-Control-Allow-Methods")).isNull();
    assertThat(response.headers().getValues("Vary")).contains("Origin");
  }
}
