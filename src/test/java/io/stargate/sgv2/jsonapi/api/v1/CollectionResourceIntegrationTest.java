package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
class CollectionResourceIntegrationTest extends AbstractNamespaceIntegrationTestBase {

  @Nested
  class ClientErrors {

    String collectionName = "col" + RandomStringUtils.randomAlphanumeric(16);

    @Test
    public void tokenMissing() {
      given()
          .contentType(ContentType.JSON)
          .body("{}")
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(401)
          .body(
              "errors[0].message",
              is(
                  "Role unauthorized for operation: Missing token, expecting one in the Token header."));
    }

    @Test
    public void malformedBody() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body("{wrong}")
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("errors[0].exceptionClass", is("JsonParseException"))
          .body("errors[0].message", is(not(blankString())));
    }

    @Test
    public void unknownCommand() {
      String json =
          """
          {
            "unknownCommand": {
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("errors[0].errorCode", is("NO_COMMAND_MATCHED"))
          .body(
              "errors[0].message",
              startsWith("No \"unknownCommand\" command found as \"CollectionCommand\""));
    }

    @Test
    public void invalidNamespaceName() {
      String json =
          """
          {
            "insertOne": {
                "document": {}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, "7_no_leading_number", collectionName)
          .then()
          .statusCode(200)
          .body("errors[0].exceptionClass", is("ConstraintViolationException"))
          .body(
              "errors[0].message",
              startsWith(
                  "Request invalid: field 'namespace' value \"7_no_leading_number\" not valid. Problem: must match "));
    }

    @Test
    public void invalidCollectionName() {
      String json =
          """
          {
            "insertOne": {
                "document": {}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, "7_no_leading_number")
          .then()
          .statusCode(200)
          .body("errors[0].exceptionClass", is("ConstraintViolationException"))
          .body(
              "errors[0].message",
              startsWith(
                  "Request invalid: field 'collection' value \"7_no_leading_number\" not valid. Problem: must match "));
    }

    @Test
    public void emptyBody() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("errors[0].exceptionClass", is("ConstraintViolationException"))
          .body("errors[0].message", is(not(blankString())));
    }
  }
}
