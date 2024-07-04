package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
class NamespaceResourceIntegrationTest extends AbstractNamespaceIntegrationTestBase {

  @Nested
  class ClientErrors {

    @Test
    public void tokenMissing() {
      final Map<String, ?> headers = getHeaders();
      given()
          .contentType(ContentType.JSON)
          .body("{}")
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body("{wrong}")
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("errors[0].exceptionClass", is("JsonApiException"))
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("errors[0].errorCode", is("COMMAND_UNKNOWN"))
          .body(
              "errors[0].message",
              startsWith(
                  "Provided command unknown: \"unknownCommand\" not one of \"NamespaceCommand\"s: known commands are [createCollection"));
    }

    @Test
    public void invalidNamespaceName() {
      String json =
          """
              {
                "createCollection": {
                    "name": "ignore_me"
                }
              }
              """;

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(NamespaceResource.BASE_PATH, "7_no_leading_number")
          .then()
          .statusCode(200)
          .body("errors[0].errorCode", is("COMMAND_FIELD_INVALID"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message",
              startsWith(
                  "Request invalid: field 'namespace' value \"7_no_leading_number\" not valid. Problem: must match "));
    }

    @Test
    public void emptyBody() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("errors[0].errorCode", is("COMMAND_FIELD_INVALID"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message",
              startsWith(
                  "Request invalid: field 'command' value `null` not valid. Problem: must not be null"));
    }
  }
}
