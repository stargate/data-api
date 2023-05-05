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
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class HttpStatusCodeIntegrationTest extends AbstractCollectionIntegrationTestBase {
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
        .body(
            "errors[0].message",
            endsWith(
                "INVALID_ARGUMENT: table %s.%s does not exist"
                    .formatted(namespaceName, "badCollection")))
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
