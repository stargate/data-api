package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.blankString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.hamcrest.core.AnyOf;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class HttpStatusCodeIntegrationTest extends AbstractCollectionIntegrationTestBase {

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class CollectionResourceStatusCode {
    @Test
    public void unauthenticated() {
      String json =
          """
            {
              "find": {
                "options" : {
                  "limit" : 1
                }
              }
            }
            """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "invalid token")
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(401)
          .body("errors", is(notNullValue()))
          .body("errors[0].message", endsWith("UNAUTHENTICATED: Invalid token"));
    }

    @Test
    public void regularError() {
      String json =
          """
            {
              "find": {
                "options" : {
                  "limit" : 1
                }
              }
            }
            """;
      AnyOf<String> anyOf =
          AnyOf.anyOf(
              endsWith(
                  "INVALID_ARGUMENT: table %s.%s does not exist"
                      .formatted(namespaceName, "badCollection")),
              endsWith("INVALID_ARGUMENT: table %s does not exist".formatted("badCollection")));
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, "badCollection")
          .then()
          .statusCode(200)
          .body("errors", is(notNullValue()))
          .body("errors[0].message", is(not(blankString())))
          .body("errors[0].message", anyOf)
          .body("errors[0].exceptionClass", is("StatusRuntimeException"));
    }

    @Test
    public void resourceNotFound() {
      String json =
          """
            {
              "find": {
                "options" : {
                  "limit" : 1
                }
              }
            }
            """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post("/unknown/{namespace}/{collection}", namespaceName, collectionName)
          .then()
          .statusCode(404);
    }

    @Test
    public void methodNotFound() {
      String json =
          """
            {
              "find": {
                "options" : {
                  "limit" : 1
                }
              }
            }
            """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .get(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(405);
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class NamespaceResourceStatusCode {
    @Test
    public void unauthenticated() {
      String json =
          """
            {
              "createCollection": {
                  "name": "ignore_me"
              }
            }
            """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "invalid token")
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(401)
          .body("errors", is(notNullValue()))
          .body("errors[0].message", endsWith("UNAUTHENTICATED: Invalid token"));
    }

    @Test
    public void regularError() {
      String json =
          """
             {
              "createCollection": {
                "name": "ignore_me"
              }
             }
             """;
      AnyOf<String> anyOf =
          AnyOf.anyOf(
              endsWith("INVALID_ARGUMENT: Keyspace '%s' doesn't exist".formatted("badNamespace")),
              endsWith(
                  "INVALID_ARGUMENT: Unknown namespace '%s', you must create it first."
                      .formatted("badNamespace")));
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(NamespaceResource.BASE_PATH, "badNamespace")
          .then()
          .statusCode(200)
          .body("errors", is(notNullValue()))
          .body("errors[0].message", is(not(blankString())))
          .body("errors[0].message", anyOf)
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void resourceNotFound() {
      String json =
          """
            {
                "createCollection": {
                    "name": "ignore_me"
                }
            }
            """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post("/unknown/{namespace}", namespaceName)
          .then()
          .statusCode(404);
    }

    @Test
    public void methodNotFound() {
      String json =
          """
            {
              "createCollection": {
                "name": "ignore_me"
              }
            }
            """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .get(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(405);
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class GeneralResourceStatusCode {
    @Test
    public void unauthenticated() {
      String json =
          """
            {
              "createNamespace": {
                  "name": "ignore_me"
              }
            }
            """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "invalid token")
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(401)
          .body("errors", is(notNullValue()))
          .body("errors[0].message", endsWith("UNAUTHENTICATED: Invalid token"));
    }

    @Test
    public void resourceNotFound() {
      String json =
          """
            {
              "createNamespace": {
                "name": "ignore_me"
              }
            }
            """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post("/unknown/{namespace}", namespaceName)
          .then()
          .statusCode(404);
    }

    @Test
    public void methodNotFound() {
      String json =
          """
              {
                "createNamespace": {
                  "name": "ignore_me"
                }
              }
            """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .get(GeneralResource.BASE_PATH)
          .then()
          .statusCode(405);
    }
  }
}
