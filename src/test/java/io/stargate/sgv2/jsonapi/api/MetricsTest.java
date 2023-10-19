package io.stargate.sgv2.jsonapi.api;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.v1.CollectionResource;
import io.stargate.sgv2.jsonapi.api.v1.GeneralResource;
import java.util.List;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class MetricsTest {

  @Test
  public void unauthorizedGeneralResource() {
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
            line -> assertThat(line).containsAnyOf("uri=\"/v1\"", "uri=\"/v1/{collection}\""));
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
        .post(CollectionResource.BASE_PATH, "collection")
        .then()
        .statusCode(401);

    String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
    List<String> httpMetrics =
        metrics.lines().filter(line -> line.startsWith("http_server_requests_seconds")).toList();

    assertThat(httpMetrics)
        .allSatisfy(
            line -> assertThat(line).containsAnyOf("uri=\"/v1\"", "uri=\"/v1/{collection}\""));
  }
}
