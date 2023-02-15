package io.stargate.sgv2.jsonapi.api;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.v1.CollectionResource;
import io.stargate.sgv2.jsonapi.api.v1.NamespaceResource;
import java.util.List;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class UnauthorizedMetricsTest {

  @Test
  public void namespaceResource() {
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
        .post(NamespaceResource.BASE_PATH, "keyspace")
        .then()
        .statusCode(200);

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
  }

  @Test
  public void collectionResource() {
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
        .statusCode(200);

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
  }
}
