package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
class GeneralResourceIntegrationTest extends AbstractKeyspaceIntegrationTestBase {

  @BeforeAll
  public static void enableLog() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @Nested
  class ClientErrors {

    @Test
    public void tokenMissing() {

      given() // No headers added on purpose
          .contentType(ContentType.JSON)
          .body("{}")
          .when()
          .post(GeneralResource.BASE_PATH)
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
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("REQUEST_NOT_JSON"))
          .body("errors[0].exceptionClass", is("RequestException"))
          .body(
              "errors[0].message",
              containsString("Request not valid JSON, problem: Unexpected character"));
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
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("RequestException"))
          .body("errors[0].errorCode", is("UNKNOWN_COMMAND"))
          .body(
              "errors[0].message",
              startsWith(
                  "Command 'unknownCommand' is not a General Command recognized by Data API."))
          .body(
              "errors[0].message",
              containsString("Data API supports following General Commands: [createKeyspace,"));
    }

    @Test
    public void emptyBody() {
      givenHeaders()
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("COMMAND_FIELD_INVALID"))
          .body("errors[0].exceptionClass", is("RequestException"))
          .body(
              "errors[0].message",
              startsWith("Command field 'command' value `null` not valid: must not be null"));
    }
  }
}
