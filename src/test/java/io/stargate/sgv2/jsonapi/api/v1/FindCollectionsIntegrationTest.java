package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

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
class FindCollectionsIntegrationTest extends CqlEnabledIntegrationTestBase {

  @BeforeAll
  public static void enableLog() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @Nested
  class FindCollections {

    @Test
    public void happyPath() {
      // create first
      String collection = "col" + RandomStringUtils.randomNumeric(16);
      String json =
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
          .body(json)
          .when()
          .post(NamespaceResource.BASE_PATH, keyspaceId.asInternal())
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
          .post(NamespaceResource.BASE_PATH, keyspaceId.asInternal())
          .then()
          .statusCode(200)
          .body("status.collections", hasSize(1))
          .body("status.collections", hasItem(collection));
    }

    @Test
    public void emptyNamespace() {
      // create namespace first
      String namespace = "nam" + RandomStringUtils.randomNumeric(16);
      String json =
          """
          {
            "createNamespace": {
              "name": "%s"
            }
          }
          """
              .formatted(namespace);

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
          .post(NamespaceResource.BASE_PATH, namespace)
          .then()
          .statusCode(200)
          .body("status.collections", hasSize(0));

      // cleanup
      json =
          """
          {
            "dropNamespace": {
              "name": "%s"
            }
          }
          """
              .formatted(namespace);

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

    @Test
    public void systemKeyspace() {
      // then find
      String json =
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
          .post(NamespaceResource.BASE_PATH, "system")
          .then()
          .statusCode(200)
          .body("status.collections", hasSize(0));
    }

    @Test
    public void notExistingNamespace() {
      // then find
      String json =
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
          .post(NamespaceResource.BASE_PATH, "should_not_be_there")
          .then()
          .statusCode(200)
          .body("errors[0].errorCode", is("NAMESPACE_DOES_NOT_EXIST"))
          .body(
              "errors[0].message",
              is("Unknown namespace should_not_be_there, you must create it first."));
    }
  }
}
