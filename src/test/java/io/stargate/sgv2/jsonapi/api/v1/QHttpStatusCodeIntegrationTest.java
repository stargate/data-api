package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsErrorWithStatus;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.containsString;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.constants.ErrorObjectV2Constants;
import io.stargate.sgv2.jsonapi.exception.ErrorFamily;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.hamcrest.core.AnyOf;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
public class QHttpStatusCodeIntegrationTest extends AbstractCollectionIntegrationTestBase {

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
          .headers(getInvalidHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(401)
          .body("$", responseIsError())
          .body("errors[0].message", endsWith("UNAUTHENTICATED: Invalid token"));
    }

    @Test
    public void unauthenticatedNamespaceResource() {
      String json =
          """
                {
                    "createNamespace": {
                        "name": "unAuthenticated"
                    }
                }
                """;
      // NOTE: Checking the status message here to test the intersection of error and status
      given()
          .headers(getInvalidHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(401)
          .body("$", responseIsErrorWithStatus())
          .body("errors[0].message", endsWith("UNAUTHENTICATED: Invalid token"))
          .body(
              "status.warnings[0]",
              hasEntry(ErrorObjectV2Constants.Fields.FAMILY, ErrorFamily.REQUEST.name()))
          .body(
              "status.warnings[0]",
              hasEntry(ErrorObjectV2Constants.Fields.SCOPE, RequestException.Scope.WARNING.scope()))
          .body(
              "status.warnings[0]",
              hasEntry(
                  ErrorObjectV2Constants.Fields.CODE,
                  WarningException.Code.DEPRECATED_COMMAND.name()))
          .body(
              "status.warnings[0].message",
              containsString("The deprecated command is: createNamespace."))
          .body(
              "status.warnings[0].message",
              containsString("The new command to use is: createKeyspace."));
      ;
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
              endsWith("table %s.%s does not exist".formatted(keyspaceName, "badCollection")),
              endsWith("table %s does not exist".formatted("badCollection")),
              endsWith(
                  "Collection does not exist, collection name: %s".formatted("badCollection")));
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, "badCollection")
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].message", is(not(blankString())))
          .body("errors[0].message", anyOf)
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void invalidContentType() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.HTML)
          .body(
              """
                                                {
                                                  "findCollections": { }
                                                }
                                                """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(415)
          .body("$", responseIsError())
          .body("errors[0].message", is("Invalid Content-Type header"))
          .body("errors[0].errorCode", is("INVALID_CONTENT_TYPE_HEADER"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post("/unknown/{namespace}/{collection}", keyspaceName, collectionName)
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .get(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(405);
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class KeyspaceResourceStatusCode {
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
          .headers(getInvalidHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(401)
          .body("$", responseIsError())
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
              endsWith("keyspace '%s' doesn't exist".formatted("badNamespace")),
              endsWith(
                  "Unknown keyspace '%s', you must create it first".formatted("badNamespace")));
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(KeyspaceResource.BASE_PATH, "badNamespace")
          .then()
          .statusCode(200)
          .body("$", responseIsError())
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post("/unknown/{namespace}", keyspaceName)
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .get(KeyspaceResource.BASE_PATH, keyspaceName)
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
      // NOTE: Checking the status message here to test the intersection of error and status
      given()
          .headers(getInvalidHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(401)
          .body("$", responseIsErrorWithStatus())
          .body("errors[0].message", endsWith("UNAUTHENTICATED: Invalid token"))
          .body(
              "status.warnings[0]",
              hasEntry(ErrorObjectV2Constants.Fields.FAMILY, ErrorFamily.REQUEST.name()))
          .body(
              "status.warnings[0]",
              hasEntry(ErrorObjectV2Constants.Fields.SCOPE, RequestException.Scope.WARNING.scope()))
          .body(
              "status.warnings[0]",
              hasEntry(
                  ErrorObjectV2Constants.Fields.CODE,
                  WarningException.Code.DEPRECATED_COMMAND.name()))
          .body(
              "status.warnings[0].message",
              containsString("The deprecated command is: createNamespace."))
          .body(
              "status.warnings[0].message",
              containsString("The new command to use is: createKeyspace."));
      ;
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post("/unknown/{namespace}", keyspaceName)
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .get(GeneralResource.BASE_PATH)
          .then()
          .statusCode(405);
    }
  }
}
