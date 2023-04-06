package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.CqlEnabledIntegrationTestBase;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
class DropNamespaceIntegrationTest extends CqlEnabledIntegrationTestBase {

  @BeforeAll
  public static void enableLog() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @Nested
  class DropNamespace {

    @Test
    public final void happyPath() {
      String json =
          """
          {
            "dropNamespace": {
              "name": "%s"
            }
          }
          """
              .formatted(keyspaceId.asInternal());

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      // ensure it's dropped
      json =
          """
              {
                "findNamespaces": {
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
          .body("status.existingNamespaces", not(hasItem(keyspaceId.asInternal())));
    }

    @Test
    public final void withExistingCollection() {
      String keyspace = "k%s".formatted(RandomStringUtils.randomAlphanumeric(8)).toLowerCase();
      String collection = "c%s".formatted(RandomStringUtils.randomAlphanumeric(8)).toLowerCase();

      String createNamespace =
          """
              {
                "createNamespace": {
                  "name": "%s"
                }
              }
              """
              .formatted(keyspace);
      String createCollection =
          """
              {
                "createCollection": {
                  "name": "%s"
                }
              }
              """
              .formatted(collection);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(createNamespace)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(createCollection)
          .when()
          .post(NamespaceResource.BASE_PATH, keyspace)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      String json =
          """
          {
            "dropNamespace": {
              "name": "%s"
            }
          }
          """
              .formatted(keyspace);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      // ensure it's dropped
      json =
          """
              {
                "findNamespaces": {
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
          .body("status.existingNamespaces", not(hasItem(keyspace)));
    }

    @Test
    public final void notExisting() {
      String json =
          """
          {
            "dropNamespace": {
              "name": "whatever_not_there"
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
          .body("status.ok", is(1));
    }
  }
}
