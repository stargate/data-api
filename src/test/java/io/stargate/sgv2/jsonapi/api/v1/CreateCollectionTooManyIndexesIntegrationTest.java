package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

/**
 * Separate integration test from {@code CreateCollectionIntegrationTest} to test case of too many
 * Indexes being created: that is, cannot create enough indexes for a new Collection.
 */
@QuarkusIntegrationTest
@QuarkusTestResource(CreateCollectionTooManyIndexesIntegrationTest.MyTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class CreateCollectionTooManyIndexesIntegrationTest extends AbstractNamespaceIntegrationTestBase {
  // Defaults are changed in `StargateTestResource`, need override to reset back to defaults:
  public static class MyTestResource extends DseTestResource {
    // We need 10 indexes per collection, so set to 20 to allow 2 collections
    // (and leave max Collection IT setting at 10).
    @Override
    public int getIndexesPerDBOverride() {
      return 20;
    }
  }

  @Nested
  @Order(1)
  class TooManyIndexes {
    @Test
    public void enforceMaxCollections() {
      // Don't use auto-generated namespace that rest of the test uses
      final String NS = "ns_too_many_indexes";
      createNamespace(NS);
      final String createTemplate =
          """
              {
                "createCollection": {
                  "name": "tooManyIx_%d"
                }
              }
              """;

      // First create 2 collections, should work fine
      for (int i = 1; i <= 2; ++i) {
        String json = createTemplate.formatted(i);
        given()
            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
            .contentType(ContentType.JSON)
            .body(json)
            .when()
            .post(NamespaceResource.BASE_PATH, NS)
            .then()
            .statusCode(200)
            .body("status.ok", is(1));
      }
      // And then failure
      String json = createTemplate.formatted(99);
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(NamespaceResource.BASE_PATH, NS)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              matchesPattern(
                  "Too many indexes: cannot create a new collection; \\d+ indexes already created in database, maximum 20"))
          .body("errors[0].errorCode", is("TOO_MANY_INDEXES"))
          .body("errors[0].exceptionClass", is("JsonApiException"));

      // But then verify that re-creating an existing one should still succeed
      // (if using same settings)
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(createTemplate.formatted(1))
          .when()
          .post(NamespaceResource.BASE_PATH, NS)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
    }
  }
}
