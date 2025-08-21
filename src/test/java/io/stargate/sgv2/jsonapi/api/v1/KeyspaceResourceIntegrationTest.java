package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
class KeyspaceResourceIntegrationTest extends AbstractKeyspaceIntegrationTestBase {

  @Nested
  class ClientErrors {

    @Test
    public void tokenMissing() {
      given() // Headers omitted on purpose
          .contentType(ContentType.JSON)
          .body("{}")
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(401)
          .body("$", responseIsError())
          .body(
              "errors[0].message",
              is(
                  "Role unauthorized for operation: Missing token, expecting one in the Token header."));
    }

    @Test
    public void malformedBody() {
      givenHeadersAndJson("{wrong}")
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].message", is(not(blankString())));
    }

    @Test
    public void unknownCommand() {
      givenHeadersAndJson(
              """
          {
            "unknownCommand": {
            }
          }
          """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("COMMAND_UNKNOWN"))
          .body(
              "errors[0].message",
              startsWith(
                  "Provided command unknown: \"unknownCommand\" not one of \"KeyspaceCommand\"s: known commands are ["));
    }

    @Test
    public void emptyBody() {
      givenHeaders()
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("COMMAND_FIELD_INVALID"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message",
              startsWith(
                  "Request invalid: field 'command' value `null` not valid. Problem: must not be null"));
    }
  }
}
