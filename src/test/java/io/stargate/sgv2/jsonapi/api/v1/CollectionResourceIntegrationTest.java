package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
class CollectionResourceIntegrationTest extends AbstractKeyspaceIntegrationTestBase {
  @Nested
  class ClientErrors {
    String collectionName = "col" + RandomStringUtils.insecure().nextAlphanumeric(16);

    @Test
    public void tokenMissing() {
      given() // NOTE: not passing headers, on purpose
          .contentType(ContentType.JSON)
          .body("{}")
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
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
      givenHeadersAndJson("wrong")
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          // 10-Jul-2024, tatu: As per [data-api#1216], should be 400, not 200
          //  (we want to return 4xx because we cannot actually process the request
          //  as JSON is unparseable) but right now this is not working for some reason.
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("INVALID_REQUEST_NOT_JSON"))
          .body(
              "errors[0].message",
              startsWith("Request invalid, cannot parse as JSON: underlying problem:"))
          .body("errors[0].message", containsString("Unrecognized token 'wrong'"));
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
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("COMMAND_UNKNOWN"))
          .body(
              "errors[0].message",
              startsWith(
                  "Provided command unknown: \"unknownCommand\" not one of \"CollectionCommand\"s: known commands are ["));
    }

    @Test
    public void unknownCommandField() {
      givenHeadersAndJson(
              """
              {
                "findOne": {
                    "unknown": "value"
                }
              }
              """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("INVALID_REQUEST_UNKNOWN_FIELD"))
          .body("errors[0].message", startsWith("Request invalid, unrecognized JSON field"))
          .body("errors[0].message", containsString("\"unknown\" not one of known fields"))
          .body(
              "errors[0].message",
              containsString("(\"filter\", \"options\", \"projection\", \"sort\")"));
    }

    @Test
    public void emptyBody() {
      // Note: no body specified
      givenHeaders()
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("COMMAND_FIELD_INVALID"))
          .body(
              "errors[0].message",
              startsWith("Request invalid: field 'command' value `null` not valid"));
    }
  }
}
