package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class FindCollectionsIntegrationTest extends AbstractCollectionIntegrationTestBase {

  @Nested
  @Order(1)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FindCollections {

    @Test
    @Order(1)
    public void happyPath() {
      // create first
      String json =
          """
                      {
                        "createCollection": {
                          "name": "%s"
                        }
                      }
                      """
              .formatted("collection1");

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      // then find
      json =
          """
                      {
                        "findCollections": {
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
          .body("status.collections", hasSize(greaterThanOrEqualTo(1)))
          .body("status.collections", hasItem("collection1"));
    }

    @Test
    @Order(1)
    public void happyPathWithExplain() {
      String json =
          """
                      {
                        "createCollection": {
                          "name": "%s",
                          "options": {
                            "vector": {
                              "size": 5,
                              "function": "cosine"
                            }
                          }
                        }
                      }
                      """
              .formatted("collection2");

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      String expected1 =
          """
                      {
                        "name": "%s"
                      }
                        """
              .formatted("collection1");

      String expectedDefault =
          """
                      {
                        "name": "%s"
                      }
                        """
              .formatted(collectionName);

      String expected2 =
          """
                      {
                          "name": "%s",
                          "options": {
                            "vector": {
                              "size": 5,
                              "function": "cosine"
                            }
                          }
                        }
                        """
              .formatted("collection2");

      json =
          """
                      {
                        "findCollections": {
                          "options": {
                            "explain" : true
                          }
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
          .body("status.collections", hasSize(3))
          .body(
              "status.collections",
              containsInAnyOrder(
                  jsonEquals(expectedDefault), jsonEquals(expected1), jsonEquals(expected2)));
    }
  }

  @Nested
  @Order(2)
  class Metrics {
    @Test
    public void checkMetrics() {
      FindCollectionsIntegrationTest.super.checkMetrics("FindCollectionsCommand");
    }
  }
}
