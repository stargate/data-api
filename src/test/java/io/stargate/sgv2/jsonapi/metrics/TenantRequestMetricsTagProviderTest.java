package io.stargate.sgv2.jsonapi.metrics;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.metrics.MetricsConstants.UNKNOWN_VALUE;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import io.stargate.sgv2.jsonapi.api.v1.GeneralResource;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import jakarta.ws.rs.core.HttpHeaders;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for the user agent tag {@link TenantRequestMetricsTagProvider} adds to request metrics. */
@QuarkusTest
@TestProfile(TenantRequestMetricsTagProviderTest.UserAgentTagProfile.class)
public class TenantRequestMetricsTagProviderTest {

  private static final String FULL_AGENT = "langflow/1.4.2 langchain/0.3.59 astrapy/2.0.1";
  private static final String COMMAND = "{\"noSuchCommand\": {}}";

  /** The user agent tag is off by default. */
  public static class UserAgentTagProfile implements QuarkusTestProfile {

    @Override
    public boolean disableGlobalTestResources() {
      return true;
    }

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.of(
          "stargate.metrics.tenant-request-counter.user-agent-tag-enabled", "true");
    }
  }

  @Test
  public void productWhenRequestContext() {
    postCommand(FULL_AGENT).statusCode(200);
    assertUserAgentTag("langflow");
  }

  @Test
  public void fullAgentWhenNoRequestContext() {
    // no token, the request is rejected before the RequestContext is created
    given()
        .contentType(ContentType.JSON)
        .header(HttpHeaders.USER_AGENT, FULL_AGENT)
        .body(COMMAND)
        .when()
        .post(GeneralResource.BASE_PATH)
        .then()
        .statusCode(401);

    assertUserAgentTag(FULL_AGENT);
  }

  @Test
  public void unknownWhenNoAgent() {
    postCommand("").statusCode(200);
    assertUserAgentTag(UNKNOWN_VALUE);
  }

  private ValidatableResponse postCommand(String userAgent) {
    return given()
        .contentType(ContentType.JSON)
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "token")
        .header(HttpHeaders.USER_AGENT, userAgent)
        .body(COMMAND)
        .when()
        .post(GeneralResource.BASE_PATH)
        .then();
  }

  private void assertUserAgentTag(String expected) {
    String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();

    assertThat(metrics.lines().filter(line -> line.startsWith("http_server_requests_")).toList())
        .describedAs("user agent tag for %s", expected)
        .anyMatch(line -> line.contains("user_agent=\"%s\"".formatted(expected)));
  }
}
