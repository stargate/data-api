package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.blankString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.CqlEnabledIntegrationTestBase;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NamespaceResourceIntegrationTest extends CqlEnabledIntegrationTestBase {

  @BeforeAll
  public static void enableLog() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @Nested
  class PostCommand {

    @Test
    public void happyPath() {
      String json =
          String.format(
              """
              {
                "createCollection": {
                  "name": "%s"
                }
              }
              """,
              "col" + RandomStringUtils.randomNumeric(16));
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(NamespaceResource.BASE_PATH, keyspaceId.asInternal())
          .then()
          .statusCode(200);
    }
  }

  @Nested
  class ClientErrors {

    @Test
    public void tokenMissing() {
      given()
          .contentType(ContentType.JSON)
          .body("{}")
          .when()
          .post(NamespaceResource.BASE_PATH, keyspaceId.asInternal())
          .then()
          .statusCode(200)
          .body(
              "errors[0].message",
              is(
                  "Role unauthorized for operation: Missing token, expecting one in the X-Cassandra-Token header."));
    }

    @Test
    public void malformedBody() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body("{wrong}")
          .when()
          .post(NamespaceResource.BASE_PATH, keyspaceId.asInternal())
          .then()
          .statusCode(200)
          .body("errors[0].message", is(not(blankString())))
          .body("errors[0].exceptionClass", is("WebApplicationException"))
          .body("errors[1].message", is(not(blankString())))
          .body("errors[1].exceptionClass", is("JsonParseException"));
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
          .post(NamespaceResource.BASE_PATH, keyspaceId.asInternal())
          .then()
          .statusCode(200)
          .body("errors[0].message", startsWith("Could not resolve type id 'unknownCommand'"))
          .body("errors[0].exceptionClass", is("InvalidTypeIdException"));
    }

    @Test
    public void emptyBody() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .when()
          .post(NamespaceResource.BASE_PATH, keyspaceId.asInternal())
          .then()
          .statusCode(200)
          .body("errors[0].message", is(not(blankString())))
          .body("errors[0].exceptionClass", is("ConstraintViolationException"));
    }
  }
}
