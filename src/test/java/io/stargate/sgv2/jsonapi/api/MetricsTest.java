package io.stargate.sgv2.jsonapi.api;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.blankString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.v1.CollectionResource;
import io.stargate.sgv2.jsonapi.api.v1.GeneralResource;
import io.stargate.sgv2.jsonapi.api.v1.NamespaceResource;
import java.util.List;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class MetricsTest {

  @Test
  public void unauthorizedNamespaceResource() {
    String json =
        """
        {
          "createCollection": {
            "name": "whatever"
          }
        }
        """;

    // ensure namespace not in tags when no auth token used
    given()
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(GeneralResource.BASE_PATH)
        .then()
        .statusCode(401);

    String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
    List<String> httpMetrics =
        metrics.lines().filter(line -> line.startsWith("http_server_requests_seconds")).toList();
    assertThat(httpMetrics)
        .allSatisfy(
            line ->
                assertThat(line)
                    .containsAnyOf(
                        "uri=\"/v1\"",
                        "uri=\"/v1/{namespace}\"",
                        "uri=\"/v1/{namespace}/{collection}\""));

    httpMetrics =
        metrics
            .lines()
            .filter(
                line ->
                    line.startsWith("http_server_requests_custom_seconds_count")
                        && line.contains("command=\"unknown\"")
                        && line.contains("error=\"true\"")
                        && line.contains("statusCode=\"401\""))
            .toList();
    assertThat(httpMetrics).hasSize(1);
  }

  @Test
  public void invalidCommandNamespaceResourceMetrics() {
    String json = """
        {
          "createCollection": {
          }
        }
        """;
    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(NamespaceResource.BASE_PATH, "namespaceName")
        .then()
        .statusCode(200);

    String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
    List<String> httpMetrics =
        metrics
            .lines()
            .filter(
                line ->
                    line.startsWith("http_server_requests_custom_seconds_count")
                        && line.contains("command=\"createCollection\"")
                        && line.contains("error=\"true\"")
                        && line.contains("statusCode=\"200\""))
            .toList();
    assertThat(httpMetrics).hasSize(1);
  }

  @Test
  public void unauthorizedCollectionResource() {
    String json = """
        {
          "find": {
          }
        }
        """;

    // ensure namespace not in tags when no auth token used
    given()
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(CollectionResource.BASE_PATH, "keyspace", "collection")
        .then()
        .statusCode(401);

    String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
    List<String> httpMetrics =
        metrics.lines().filter(line -> line.startsWith("http_server_requests_seconds")).toList();

    assertThat(httpMetrics)
        .allSatisfy(
            line ->
                assertThat(line)
                    .containsAnyOf(
                        "uri=\"/v1\"",
                        "uri=\"/v1/{namespace}\"",
                        "uri=\"/v1/{namespace}/{collection}\""));

    httpMetrics =
        metrics
            .lines()
            .filter(
                line ->
                    line.startsWith("http_server_requests_custom_seconds_count")
                        && line.contains("command=\"unknown\"")
                        && line.contains("error=\"true\"")
                        && line.contains("statusCode=\"401\""))
            .toList();
    assertThat(httpMetrics).hasSize(1);
  }

  @Test
  public void invalidCollectionResourceMetrics() {
    String json = """
        {
          "bad_command": {
          }
        }
        """;
    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(CollectionResource.BASE_PATH, "namespaceName", "collectionName")
        .then()
        .statusCode(200);

    String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
    List<String> httpMetrics =
        metrics
            .lines()
            .filter(
                line ->
                    line.startsWith("http_server_requests_custom_seconds_count")
                        && line.contains("command=\"bad_command\"")
                        && line.contains("error=\"true\"")
                        && line.contains("statusCode=\"200\""))
            .toList();
    assertThat(httpMetrics)
        .allSatisfy(
            line -> {
              assertThat(line).contains("command=\"bad_command\"");
              assertThat(line).contains("statusCode=\"200\"");
              assertThat(line).contains("error=\"true\"");
            });
  }

  @Test
  public void unauthorizedGeneralResource() {
    String json =
        """
        {
          "createNamespace": {
          "name": "purchase_database"
          }
        }
        """;

    // ensure namespace not in tags when no auth token used
    given()
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(GeneralResource.BASE_PATH)
        .then()
        .statusCode(401);

    String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
    List<String> httpMetrics =
        metrics.lines().filter(line -> line.startsWith("http_server_requests_seconds")).toList();

    assertThat(httpMetrics)
        .allSatisfy(
            line ->
                assertThat(line)
                    .containsAnyOf(
                        "uri=\"/v1\"",
                        "uri=\"/v1/{namespace}\"",
                        "uri=\"/v1/{namespace}/{collection}\""));

    httpMetrics =
        metrics
            .lines()
            .filter(
                line ->
                    line.startsWith("http_server_requests_custom_seconds_count")
                        && line.contains("command=\"unknown\"")
                        && line.contains("error=\"true\"")
                        && line.contains("statusCode=\"401\""))
            .toList();
    assertThat(httpMetrics).hasSize(1);
  }

  @Test
  public void invalidCommandGeneralResourceMetrics() {
    String json = """
        {
          "createNamespace": {
          }
        }
        """;

    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(GeneralResource.BASE_PATH)
        .then()
        .statusCode(200)
        .body("errors[0].message", is(not(blankString())))
        .body("errors[0].exceptionClass", is("ConstraintViolationException"));

    String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
    List<String> httpMetrics =
        metrics
            .lines()
            .filter(
                line ->
                    line.startsWith("http_server_requests_custom_seconds_count")
                        && line.contains("command=\"createNamespace\"")
                        && line.contains("error=\"true\"")
                        && line.contains("statusCode=\"200\""))
            .toList();
    assertThat(httpMetrics).hasSize(1);
  }
}
