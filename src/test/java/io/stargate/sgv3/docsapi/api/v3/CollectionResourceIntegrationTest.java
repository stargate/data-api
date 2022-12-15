package io.stargate.sgv3.docsapi.api.v3;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
class CollectionResourceIntegrationTest {

  @BeforeAll
  public static void enableLog() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @Nested
  class FindOne {

    @Test
    public void happyPath() {
      String json =
          """
              {
                "findOne": {
                  "sort": ["user.age"]
                }
              }
              """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, "database", "collection")
          .then()
          .statusCode(200)
          .body("errors", is(not(empty())))
          .body("errors[0].errorCode", is("COMMAND_NOT_IMPLEMENTED"))
          .body("errors[0].message", is("The command FindOneCommand is not implemented."));
    }
  }

  @Nested
  class InsertOne {

    @Test
    public void emptyDocument() {
      String json =
          """
          {
            "insertOne": {
              "document": {
              }
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, "database", "collection")
          .then()
          .statusCode(200);
    }

    @Test
    @DisabledIfSystemProperty(
        named = "testing.package.type",
        matches = "native",
        disabledReason =
            "[V2 exception mappers map to ApiError which is not registered for refection](https://github.com/riptano/sgv3-docsapi/issues/8)")
    public void notValidDocumentMissing() {
      String json =
          """
              {
                "insertOne": {
                }
              }
              """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, "database", "collection")
          .then()
          .statusCode(400);
    }
  }
}
